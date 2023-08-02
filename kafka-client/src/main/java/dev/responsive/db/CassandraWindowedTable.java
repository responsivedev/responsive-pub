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
import static dev.responsive.db.ColumnNames.DATA_KEY;
import static dev.responsive.db.ColumnNames.DATA_VALUE;
import static dev.responsive.db.ColumnNames.EPOCH;
import static dev.responsive.db.ColumnNames.OFFSET;
import static dev.responsive.db.ColumnNames.PARTITION_KEY;
import static dev.responsive.db.ColumnNames.WINDOW_START;
import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import dev.responsive.db.MetadataStatements.MetadataKeys;
import dev.responsive.kafka.store.ResponsiveWindowStore;
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWindowedTable implements RemoteWindowedTable {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowedTable.class);
  private static final Bytes METADATA_KEY
      = Bytes.wrap("_metadata".getBytes(StandardCharsets.UTF_8));

  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";
  private static final String W_FROM_BIND = "wf";
  private static final String W_TO_BIND = "wt";

  private final CassandraClient client;
  private final Predicate<Stamped<Bytes>> withinRetention;

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

  private final MetadataStatements metadataStatements;

  public CassandraWindowedTable(
      final CassandraClient client,
      final Predicate<Stamped<Bytes>> withinRetention
  ) {
    this.client = client;
    this.withinRetention = withinRetention;
    insert = new ConcurrentHashMap<>();
    fetch = new ConcurrentHashMap<>();
    fetchAll = new ConcurrentHashMap<>();
    fetchRange = new ConcurrentHashMap<>();
    backFetch = new ConcurrentHashMap<>();
    backFetchAll = new ConcurrentHashMap<>();
    backFetchRange = new ConcurrentHashMap<>();
    metadataStatements = new MetadataStatements(client, new MetadataKeys() {
      @Override
      public <T extends OngoingWhereClause<T>> T addMetaColumnsToWhere(final T builder) {
        return builder
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)));
      }

      @Override
      public RegularInsert addKeyColumnsToInitMetadata(final RegularInsert insert) {
        return insert
            .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
            .value(WINDOW_START.column(), WINDOW_START.literal(0L));
      }
    });
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
    client.execute(SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
        .withClusteringColumn(WINDOW_START.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
        .withColumn(OFFSET.column(), DataTypes.BIGINT)
        .withColumn(EPOCH.column(), DataTypes.BIGINT)
        .build()
    );
  }

  @Override
  public void prepare(final String tableName) {
    metadataStatements.prepare(tableName);
    insert.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .insertInto(tableName)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(WINDOW_START.column(), bindMarker(WINDOW_START.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    fetch.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
            .build()
    ));

    fetchAll.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .build()
    ));

    fetchRange.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .build()
    ));

    backFetch.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    ));

    backFetchAll.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    ));

    backFetchRange.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    ));
  }

  @Override
  public CassandraClient getClient() {
    return client;
  }

  /**
   * Inserts data into {@code table}. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param table         the table to insert into
   * @param partitionKey  the partitioning key
   * @param key           the data key
   * @param value         the data value
   *
   * @return a statement that, when executed, will insert the row
   *         matching {@code partitionKey} and {@code key} in the
   *         {@code table} with value {@code value}
   */
  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final String table,
      final int partitionKey,
      final Stamped<Bytes> key,
      final byte[] value
  ) {
    return insert.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(bytes(key).get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  @Override
  public BoundStatement delete(
      final String table,
      final int partitionKey,
      final Stamped<Bytes> key
  ) {
    throw new UnsupportedOperationException("Cannot delete windowed data using the delete API");
  }

  @Override
  public Bytes bytes(final Stamped<Bytes> key) {
    return key.key;
  }

  @Override
  public Stamped<Bytes> keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();
    final int size = key.length - TIMESTAMP_SIZE;

    final ByteBuffer buffer = ByteBuffer.wrap(key);
    final long startTs = buffer.getLong(size);
    final Bytes kBytes = Bytes.wrap(Arrays.copyOfRange(key, 0, size));

    return new Stamped<>(kBytes, startTs);
  }

  @Override
  public boolean retain(final Stamped<Bytes> key) {
    return withinRetention.test(key);
  }

  @Override
  public MetadataStatements metadata() {
    return metadataStatements;
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param key       the data key
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.kv(
        result.iterator(),
        CassandraWindowedTable::windowRows
    );
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param key       the data key
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.kv(
        result.iterator(),
        CassandraWindowedTable::windowRows
    );
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param fromKey   the min data key (inclusive)
   * @param toKey     the max data key (exclusive)
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetchRange.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param fromKey   the min data key (inclusive)
   * @param toKey     the max data key (exclusive)
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetchRange.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }


  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetchAll.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setInstant(FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetchAll.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);

    final ResultSet result = client.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  private static KeyValue<Stamped<Bytes>, byte[]> windowRows(final Row row) {
    final long startTs = row.getInstant(WINDOW_START.column()).toEpochMilli();
    final Bytes key = Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array());

    return new KeyValue<>(
        new Stamped<>(key, startTs),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

  @Override
  public int compare(final Stamped<Bytes> o1, final Stamped<Bytes> o2) {
    return ResponsiveWindowStore.compareKeys(o1, o2);
  }
}
