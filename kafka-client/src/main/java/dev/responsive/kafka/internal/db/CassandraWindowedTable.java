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

package dev.responsive.kafka.internal.db;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_VALUE;
import static dev.responsive.kafka.internal.db.ColumnName.EPOCH;
import static dev.responsive.kafka.internal.db.ColumnName.METADATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.OFFSET;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.ROW_TYPE;
import static dev.responsive.kafka.internal.db.ColumnName.WINDOW_START;
import static dev.responsive.kafka.internal.db.RowType.DATA_ROW;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Stamped;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWindowedTable implements RemoteWindowedTable<BoundStatement> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowedTable.class);

  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";
  private static final String W_FROM_BIND = "wf";
  private static final String W_TO_BIND = "wt";

  private final String name;
  private final CassandraClient client;

  private final PreparedStatement insert;
  private final PreparedStatement delete;
  private final PreparedStatement fetchSingle;
  private final PreparedStatement fetch;
  private final PreparedStatement fetchAll;
  private final PreparedStatement fetchRange;
  private final PreparedStatement backFetch;
  private final PreparedStatement backFetchAll;
  private final PreparedStatement backFetchRange;
  private final PreparedStatement getMeta;
  private final PreparedStatement setOffset;

  public static CassandraWindowedTable create(
      final CassandraTableSpec spec,
      final CassandraClient client
  ) throws InterruptedException, TimeoutException {
    final String name = spec.tableName();

    // TODO(window): explore better data models for fetchRange/fetchAll
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
    LOG.info("Creating windowed data table {} in remote store.", name);
    final CreateTableWithOptions createTable = spec.applyOptions(createTable(name));

    client.execute(createTable.build());
    client.awaitTable(name).await(Duration.ofSeconds(60));

    final var insert = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(ROW_TYPE.column(), DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(WINDOW_START.column(), bindMarker(WINDOW_START.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    );

    final var delete = client.prepare(
        QueryBuilder
            .deleteFrom(name)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isEqualTo(bindMarker(WINDOW_START.bind())))
            .build()
    );

    final var fetchSingle = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isEqualTo(bindMarker(WINDOW_START.bind())))
            .build()
    );

    final var fetch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
            .build()
    );

    final var fetchAll = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .build()
    );

    final var fetchRange = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .build()
    );

    final var backFetch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var backFetchAll = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var backFetchRange = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var getMeta = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(EPOCH.column())
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    );

    final var setOffset = client.prepare(QueryBuilder
        .update(name)
        .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
        .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
        .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
        .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
        .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
        .build()
    );

    return new CassandraWindowedTable(
        name,
        client,
        insert,
        delete,
        fetchSingle,
        fetch,
        fetchAll,
        fetchRange,
        backFetch,
        backFetchAll,
        backFetchRange,
        getMeta,
        setOffset
    );
  }

  private static CreateTableWithOptions createTable(final String tableName) {
    return SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withClusteringColumn(ROW_TYPE.column(), DataTypes.TINYINT)
        .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
        .withClusteringColumn(WINDOW_START.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
        .withColumn(OFFSET.column(), DataTypes.BIGINT)
        .withColumn(EPOCH.column(), DataTypes.BIGINT);
  }

  public CassandraWindowedTable(
      final String name,
      final CassandraClient client,
      final PreparedStatement insert,
      final PreparedStatement delete,
      final PreparedStatement fetchSingle,
      final PreparedStatement fetch,
      final PreparedStatement fetchAll,
      final PreparedStatement fetchRange,
      final PreparedStatement backFetch,
      final PreparedStatement backFetchAll,
      final PreparedStatement backFetchRange,
      final PreparedStatement getMeta,
      final PreparedStatement setOffset
  ) {
    this.name = name;
    this.client = client;
    this.insert = insert;
    this.delete = delete;
    this.fetchSingle = fetchSingle;
    this.fetch = fetch;
    this.fetchAll = fetchAll;
    this.fetchRange = fetchRange;
    this.backFetch = backFetch;
    this.backFetchAll = backFetchAll;
    this.backFetchRange = backFetchRange;
    this.getMeta = getMeta;
    this.setOffset = setOffset;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<Stamped<Bytes>> init(
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    partitioner.all(kafkaPartition).forEach(sub -> {
      client.execute(
          QueryBuilder.insertInto(name)
              .value(PARTITION_KEY.column(), PARTITION_KEY.literal(sub))
              .value(ROW_TYPE.column(), METADATA_ROW.literal())
              .value(DATA_KEY.column(), DATA_KEY.literal(ColumnName.METADATA_KEY))
              .value(WINDOW_START.column(), WINDOW_START.literal(0L))
              .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
              .value(EPOCH.column(), EPOCH.literal(0L))
              .ifNotExists()
              .build()
      );
    });
    return LwtWriterFactory.reserveWindowed(
        this,
        client,
        partitioner,
        kafkaPartition
    );
  }

  @Override
  public MetadataRow metadata(final int partition) {
    final BoundStatement bound = getMeta
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          name, partition, result.size()));
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getLong(EPOCH.column())
      );
    }
  }

  @Override
  public BoundStatement setOffset(
      final int partition,
      final long offset
  ) {
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public long approximateNumEntries(final int partition) {
    return client.count(name(), partition);
  }

  /**
   * Inserts data into {@code table}. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param partitionKey the partitioning key
   * @param key          the data key
   * @param value        the data value
   * @param epochMillis   the timestamp of the event
   * @return a statement that, when executed, will insert the row
   *         matching {@code partitionKey} and {@code key} in the
   *         {@code table} with value {@code value}. Note that the
   *         {@code key} here is the "windowed key" which includes
   *         both the record key and also the windowStart timestamp
   */
  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final int partitionKey,
      final Stamped<Bytes> key,
      final byte[] value,
      final long epochMillis
  ) {
    return insert
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  /**
   * @param partitionKey  the partitioning key
   * @param key           the data key
   *
   * @return a statement that, when executed, will delete the row
   *         matching {@code partitionKey} and {@code key} in the
   *         {@code table}. Note that the {@code key} here is the
   *         "windowed key" which includes both the record key and
   *         also the window start timestamp
   */
  @Override
  @CheckReturnValue
  public BoundStatement delete(
      final int partitionKey,
      final Stamped<Bytes> key
  ) {
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp));
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param partition   the partition
   * @param key         the data key
   * @param windowStart the start time of the window
   * @return the value previously set
   */
  @Override
  public byte[] fetch(
      final int partition,
      final Bytes key,
      final long windowStart
  ) {
    final BoundStatement get = fetchSingle
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(windowStart));

    final List<Row> result = client.execute(get).all();
    if (result.size() > 1) {
      throw new IllegalStateException("Unexpected multiple results for point lookup");
    } else if (result.isEmpty()) {
      return null;
    } else {
      final ByteBuffer value = result.get(0).getByteBuffer(DATA_VALUE.column());
      return Objects.requireNonNull(value).array();
    }
  }

  /**
   * Retrieves the range of windows of the given {@code partitionKey} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo} from {@code table}.
   *
   * @param partition the partition
   * @param key       the data key
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   * @return the windows previously stored
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetch
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
   * Retrieves the range of window of the given {@code partitionKey} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo} from {@code table}.
   *
   * @param partition the partition
   * @param key       the data key
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetch
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
   * Retrieves the range of windows of the given {@code partitionKey} for all keys between
   * {@code fromKey} and {@code toKey} with a start time between {@code timeFrom} and {@code timeTo}
   * from {@code table}.
   *
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
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetchRange
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
   * Retrieves the range of windows of the given {@code partitionKey} for all keys between
   * {@code fromKey} and {@code toKey} with a start time between {@code timeFrom} and {@code timeTo}
   * from {@code table}.
   *
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
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetchRange
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
   * Retrieves the windows of the given {@code partitionKey} across all keys and times
   * from {@code table}.
   *
   * @param partition the partition
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = fetchAll
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
   * Retrieves the windows of the given {@code partitionKey} across all keys and times
   * from {@code table}.
   *
   * @param partition the partition
   * @param timeFrom  the min timestamp (inclusive)
   * @param timeTo    the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = backFetchAll
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

}
