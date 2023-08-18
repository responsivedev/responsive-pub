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
import static dev.responsive.db.ColumnName.OFFSET;
import static dev.responsive.db.ColumnName.PARTITION_KEY;
import static dev.responsive.db.ColumnName.ROW_TYPE;
import static dev.responsive.db.ColumnName.TIMESTAMP;
import static dev.responsive.kafka.store.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy;
import dev.responsive.db.partitioning.SubPartitioner;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraFactSchema implements RemoteKeyValueSchema {

  private static final Logger LOG = LoggerFactory.getLogger(
      CassandraFactSchema.class);

  private final CassandraClient client;

  // use ConcurrentHashMap instead of ConcurrentMap in the declaration here
  // because ConcurrentHashMap guarantees that the supplied function for
  // computeIfAbsent is invoked exactly once per invocation of the method
  // if the key is absent, else not at all. this guarantee is not present
  // in all implementations of ConcurrentMap
  private final ConcurrentHashMap<String, PreparedStatement> get;
  private final ConcurrentHashMap<String, PreparedStatement> insert;
  private final ConcurrentHashMap<String, PreparedStatement> delete;
  private final ConcurrentHashMap<String, PreparedStatement> getMeta;
  private final ConcurrentHashMap<String, PreparedStatement> setOffset;

  public CassandraFactSchema(final CassandraClient client) {
    this.client = client;
    get = new ConcurrentHashMap<>();
    insert = new ConcurrentHashMap<>();
    delete = new ConcurrentHashMap<>();
    getMeta = new ConcurrentHashMap<>();
    setOffset = new ConcurrentHashMap<>();
  }

  @Override
  public void create(final String tableName, Optional<Duration> ttl) {
    LOG.info("Creating fact data table {} in remote store.", tableName);

    final CreateTableWithOptions createTable;
    if (ttl.isPresent()) {
      final int ttlSeconds = Math.toIntExact(ttl.get().getSeconds());
      // 20 is a magic number recommended by scylla for the number of buckets
      final Duration compactionWindow = Duration.ofSeconds(ttlSeconds / 20);
      createTable = createTable(tableName).withDefaultTimeToLiveSeconds(ttlSeconds)
          .withCompaction(
              SchemaBuilder.timeWindowCompactionStrategy()
                  .withCompactionWindow(
                      compactionWindow.toMinutes(),
                      TimeWindowCompactionStrategy.CompactionWindowUnit.MINUTES)
          );
    } else {
      createTable = createTable(tableName)
          .withCompaction(SchemaBuilder.timeWindowCompactionStrategy());
    }

    // separate metadata from the main table for the fact schema, this is acceptable
    // because we don't use the metadata at all for fencing operations and writes to
    // it do not need to be atomic (transactional with the original table). we cannot
    // effectively use the same table (as we do with the normal KeyValueSchema) because
    // TWCS cannot properly compact files if there are any overwrites, which there are
    // for the metadata columns
    final CreateTableWithOptions createMetadataTable = SchemaBuilder
        .createTable(metadataTable(tableName))
        .ifNotExists()
        .withPartitionKey(ROW_TYPE.column(), DataTypes.TINYINT)
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withColumn(OFFSET.column(), DataTypes.BIGINT);

    client.execute(createTable.build());
    client.execute(createMetadataTable.build());
  }

  private CreateTableWithOptions createTable(final String tableName) {
    return SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(ROW_TYPE.column(), DataTypes.TINYINT)
        .withPartitionKey(DATA_KEY.column(), DataTypes.BLOB)
        .withColumn(TIMESTAMP.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB);
  }

  /**
   * Initializes the metadata entry for {@code table} by adding a
   * row with key {@code _metadata} and sets special columns
   * {@link ColumnName#OFFSET} and no {@link ColumnName#EPOCH}.
   *
   * <p>Note that this method is idempotent as it uses Cassandra's
   * {@code IF NOT EXISTS} functionality.
   *
   * @param table          the table that is initialized
   * @param kafkaPartition the partition to initialize
   */
  @Override
  public WriterFactory<Bytes> init(
      final String table,
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    client.execute(
        QueryBuilder.insertInto(metadataTable(table))
            .value(ROW_TYPE.column(), RowType.METADATA_ROW.literal())
            .value(PARTITION_KEY.column(), PARTITION_KEY.literal(kafkaPartition))
            .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
            .ifNotExists()
            .build()
    );

    return new FactWriterFactory<>(this);
  }

  @Override
  public MetadataRow metadata(final String table, final int partition) {
    final BoundStatement bound = getMeta.get(metadataTable(table))
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          table, partition, result.size()));
    } else {
      final long offset = result.get(0).getLong(OFFSET.column());
      LOG.info("Got offset for {}[{}]: {}", table, partition, offset);
      return new MetadataRow(offset, -1L);
    }
  }

  @Override
  public BoundStatement setOffset(
      final String table,
      final int partition,
      final long offset
  ) {
    LOG.info("Setting offset in metadata table {} for {}[{}] to {}",
        metadataTable(table), table, partition, offset);
    return setOffset.get(metadataTable(table))
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public void prepare(final String tableName) {
    insert.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .insertInto(tableName)
            .value(ROW_TYPE.column(), RowType.DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    get.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_VALUE.column())
            .where(ROW_TYPE.relation().isEqualTo(RowType.DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition (it actually  only
            // returns a single value)
            .allowFiltering()
            .build()
    ));

    delete.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .deleteFrom(tableName)
            .where(ROW_TYPE.relation().isEqualTo(RowType.DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));

    getMeta.computeIfAbsent(metadataTable(tableName), k -> client.prepare(
        QueryBuilder
            .selectFrom(metadataTable(tableName))
            .column(OFFSET.column())
            .where(ROW_TYPE.relation().isEqualTo(RowType.METADATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .build()
    ));

    setOffset.computeIfAbsent(metadataTable(tableName), k -> client.prepare(QueryBuilder
        .update(metadataTable(tableName))
        .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
        .where(ROW_TYPE.relation().isEqualTo(RowType.METADATA_ROW.literal()))
        .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
        .build()
    ));
  }

  @Override
  public CassandraClient cassandraClient() {
    return client;
  }

  /**
   * @param table         the table to delete from
   * @param partitionKey  the partitioning key
   * @param key           the data key
   *
   * @return a statement that, when executed, will delete the row
   *         matching {@code partitionKey} and {@code key} in the
   *         {@code table}
   */
  @Override
  @CheckReturnValue
  public BoundStatement delete(
      final String table,
      final int partitionKey,
      final Bytes key
  ) {
    // TODO: consider throwing an exception here as TWCS doesn't work well with deletes
    return delete.get(table)
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));
  }


  /**
   * Inserts data into {@code table}. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param table        the table to insert into
   * @param partitionKey the partitioning key
   * @param key          the data key
   * @param value        the data value
   * @param timestamp    the event time of the data
   * @return a statement that, when executed, will insert the row
   * matching {@code partitionKey} and {@code key} in the
   * {@code table} with value {@code value}
   */
  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final String table,
      final int partitionKey,
      final Bytes key,
      final byte[] value,
      long timestamp
  ) {
    return insert.get(table)
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(timestamp));
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName  the table to retrieve from
   * @param partition  the partition
   * @param key        the data key
   * @param minValidTs the minimum valid timestamp to apply semantic TTL
   * @return the value previously set
   */
  @Override
  public byte[] get(final String tableName, final int partition, final Bytes key, long minValidTs) {
    final BoundStatement get = this.get.get(tableName)
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

    final List<Row> result = client.execute(get).all();
    if (result.size() > 1) {
      throw new IllegalArgumentException();
    } else if (result.isEmpty()) {
      return null;
    } else {
      final ByteBuffer value = result.get(0).getByteBuffer(DATA_VALUE.column());
      return Objects.requireNonNull(value).array();
    }
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final String tableName,
      final int partition,
      final Bytes from,
      final Bytes to,
      long minValidTs) {
    throw new UnsupportedOperationException("range scans are not supported on Idempotent schemas.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final String tableName,
      final int partition,
      long minValidTs) {
    throw new UnsupportedOperationException("all is not supported on Idempotent schemas");
  }

  private static String metadataTable(final String tableName) {
    return tableName + "_md";
  }

}
