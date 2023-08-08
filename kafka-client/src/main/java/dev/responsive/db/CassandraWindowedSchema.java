/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.db;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static dev.responsive.db.ColumnName.DATA_KEY;
import static dev.responsive.db.ColumnName.DATA_VALUE;
import static dev.responsive.db.ColumnName.EPOCH;
import static dev.responsive.db.ColumnName.METADATA_KEY;
import static dev.responsive.db.ColumnName.OFFSET;
import static dev.responsive.db.ColumnName.PARTITION_KEY;
import static dev.responsive.db.ColumnName.ROW_TYPE;
import static dev.responsive.db.ColumnName.WINDOW_START;
import static dev.responsive.db.RowType.DATA_ROW;
import static dev.responsive.db.RowType.METADATA_ROW;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWindowedSchema implements RemoteWindowedSchema {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowedSchema.class);

  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";
  private static final String W_FROM_BIND = "wf";
  private static final String W_TO_BIND = "wt";

  private final CassandraClient client;

  // use ConcurrentHashMap instead of ConcurrentMap in the declaration here
  // because ConcurrentHashMap guarantees that the supplied function for
  // computeIfAbsent is invoked exactly once per invocation of the method
  // if the key is absent, else not at all. this guarantee is not present
  // in all implementations of ConcurrentMap
  private final ConcurrentHashMap<String, PreparedStatement> insert;
  private final ConcurrentHashMap<String, PreparedStatement> fetch;
  private final ConcurrentHashMap<String, PreparedStatement> fetchAll;
  private final ConcurrentHashMap<String, PreparedStatement> fetchRange;
  private final ConcurrentHashMap<String, PreparedStatement> backFetch;
  private final ConcurrentHashMap<String, PreparedStatement> backFetchAll;
  private final ConcurrentHashMap<String, PreparedStatement> backFetchRange;
  private final ConcurrentHashMap<String, PreparedStatement> getMeta;
  private final ConcurrentHashMap<String, PreparedStatement> setOffset;

  public CassandraWindowedSchema(final CassandraClient client) {
    this.client = client;
    insert = new ConcurrentHashMap<>();
    fetch = new ConcurrentHashMap<>();
    fetchAll = new ConcurrentHashMap<>();
    fetchRange = new ConcurrentHashMap<>();
    backFetch = new ConcurrentHashMap<>();
    backFetchAll = new ConcurrentHashMap<>();
    backFetchRange = new ConcurrentHashMap<>();
    getMeta = new ConcurrentHashMap<>();
    setOffset = new ConcurrentHashMap<>();
  }

  @Override
  public void create(final String tableName) {
    // TODO: explore better data models for fetchRange/fetchAll
    // Cassandra does not support filtering on a composite key column if
    // the previous columns in the composite are not equality filters
    // in the table below, for example, we cannot filter on WINDOW_START
    // unless both PARTITION_KEY and DATA_KEY are equality filters because
    // of the way SSTables are used in Cassandra this would be inefficient
    //
    // until we figure out a better data model (perhaps segmenting the
    // stores using an additional column to reduce the scan overhead -
    // this would also help us avoid having a partitioning key that is
    // too small in cardinality) we just filter the results to match the
    // time bounds
    LOG.info("Creating windowed data table {} in remote store.", tableName);
    client.execute(
        SchemaBuilder.createTable(tableName)
            .ifNotExists()
            .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
            .withClusteringColumn(ROW_TYPE.column(), DataTypes.TINYINT)
            .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
            .withClusteringColumn(WINDOW_START.column(), DataTypes.TIMESTAMP)
            .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
            .withColumn(OFFSET.column(), DataTypes.BIGINT)
            .withColumn(EPOCH.column(), DataTypes.BIGINT)
            .build());
  }

  @Override
  public FencingToken init(
      final String table, final SubPartitioner partitioner, final int kafkaPartition) {
    partitioner
        .all(kafkaPartition)
        .forEach(
            sub -> {
              client.execute(
                  QueryBuilder.insertInto(table)
                      .value(PARTITION_KEY.column(), PARTITION_KEY.literal(sub))
                      .value(ROW_TYPE.column(), METADATA_ROW.literal())
                      .value(DATA_KEY.column(), DATA_KEY.literal(ColumnName.METADATA_KEY))
                      .value(WINDOW_START.column(), WINDOW_START.literal(0L))
                      .value(OFFSET.column(), OFFSET.literal(-1L))
                      .value(EPOCH.column(), EPOCH.literal(0L))
                      .ifNotExists()
                      .build());
            });
    return LwtFencingToken.reserveWindowed(this, table, partitioner, kafkaPartition);
  }

  @Override
  public MetadataRow metadata(final String table, final int partition) {
    final BoundStatement bound = getMeta.get(table).bind().setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() != 1) {
      throw new IllegalStateException(
          String.format(
              "Expected exactly one offset row for %s[%s] but got %d",
              table, partition, result.size()));
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()), result.get(0).getLong(EPOCH.column()));
    }
  }

  @Override
  public BoundStatement setOffset(
      final String table, final FencingToken token, final int partition, final long offset) {
    return setOffset
        .get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public void prepare(final String tableName) {
    insert.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.insertInto(tableName)
                    .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
                    .value(ROW_TYPE.column(), DATA_ROW.literal())
                    .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
                    .value(WINDOW_START.column(), bindMarker(WINDOW_START.bind()))
                    .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
                    .build()));

    fetch.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
                    .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
                    .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
                    .build()));

    fetchAll.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .build()));

    fetchRange.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
                    .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
                    .build()));

    backFetch.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
                    .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
                    .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
                    .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
                    .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
                    .build()));

    backFetchAll.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
                    .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
                    .build()));

    backFetchRange.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
                    .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
                    .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
                    .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
                    .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
                    .build()));

    getMeta.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.selectFrom(tableName)
                    .column(EPOCH.column())
                    .column(OFFSET.column())
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
                    .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
                    .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
                    .build()));

    setOffset.computeIfAbsent(
        tableName,
        k ->
            client.prepare(
                QueryBuilder.update(tableName)
                    .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
                    .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
                    .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
                    .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
                    .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
                    .build()));
  }

  @Override
  public CassandraClient getClient() {
    return client;
  }

  /**
   * Inserts data into {@code table}. Note that this will overwrite any existing entry in the table
   * with the same key.
   *
   * @param table the table to insert into
   * @param partitionKey the partitioning key
   * @param key the data key
   * @param value the data value
   * @return a statement that, when executed, will insert the row matching {@code partitionKey} and
   *     {@code key} in the {@code table} with value {@code value}
   */
  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final String table, final int partitionKey, final Stamped<Bytes> key, final byte[] value) {
    return insert
        .get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  @Override
  public BoundStatement delete(
      final String table, final int partitionKey, final Stamped<Bytes> key) {
    throw new UnsupportedOperationException("Cannot delete windowed data using the delete API");
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param key the data key
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo) {
    final BoundStatement get =
        fetch
            .get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
            .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
            .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
            .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows);
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param key the data key
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo) {
    final BoundStatement get =
        backFetch
            .get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
            .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
            .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
            .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows);
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param fromKey the min data key (inclusive)
   * @param toKey the max data key (exclusive)
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo) {
    final BoundStatement get =
        fetchRange
            .get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
            .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
            .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo);
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param fromKey the min data key (inclusive)
   * @param toKey the max data key (exclusive)
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo) {
    final BoundStatement get =
        backFetchRange
            .get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
            .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
            .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo);
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final String tableName, final int partition, final long timeFrom, final long timeTo) {
    final BoundStatement get =
        fetchAll
            .get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
            .setInstant(FROM_BIND, Instant.ofEpochMilli(timeFrom))
            .setInstant(TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo);
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param timeFrom the min timestamp (inclusive)
   * @param timeTo the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final String tableName, final int partition, final long timeFrom, final long timeTo) {
    final BoundStatement get =
        backFetchAll.get(tableName).bind().setInt(PARTITION_KEY.bind(), partition);

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedSchema::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo);
  }

  private static KeyValue<Stamped<Bytes>, byte[]> windowRows(final Row row) {
    final long startTs = row.getInstant(WINDOW_START.column()).toEpochMilli();
    final Bytes key = Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array());

    return new KeyValue<>(
        new Stamped<>(key, startTs), row.getByteBuffer(DATA_VALUE.column()).array());
  }
}
