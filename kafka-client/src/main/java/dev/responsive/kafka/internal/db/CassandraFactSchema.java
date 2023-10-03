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
import static dev.responsive.kafka.internal.db.ColumnName.OFFSET;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.ROW_TYPE;
import static dev.responsive.kafka.internal.db.ColumnName.TIMESTAMP;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
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
          .withCompaction(compactionStrategy(compactionWindow))
          // without setting a low gc_grace_seconds, we will have to wait an
          // additional 10 days (the default gc_grace_seconds) before any SST
          // on disk is removed. setting a good value for this is somewhat a
          // "dark art"
          //
          // there's no reason to hold on to tombstones (or hints) for more than
          // the TTL since any data that was written more than ttlSeconds ago
          // does not need to be replicated to a node that has been down for the
          // entirety of ttlSeconds
          //
          // we choose 6 hours for the minimum value since that's 2x the default
          // retention for hints (hinted handoff). handoffs are disabled by default
          // in cloud, so we could theoretically set this to 0, but holding on to
          // an extra 6 hours of data when TTL is larger than 6 hours anyway isn't
          // too big of a concern (and it can always be tweaked afterwards)
          //
          // see this blog for mor information:
          // https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html
          .withGcGraceSeconds(Math.min(ttlSeconds, (int) TimeUnit.HOURS.toSeconds(6)));
    } else {
      createTable = createTable(tableName).withCompaction(compactionStrategy(null));
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

  protected CompactionStrategy<?> compactionStrategy(@Nullable final Duration compactionWindow) {
    return compactionWindow == null
        ? SchemaBuilder.timeWindowCompactionStrategy()
        : SchemaBuilder.timeWindowCompactionStrategy()
          .withCompactionWindow(
            compactionWindow.toMinutes(),
            TimeWindowCompactionStrategy.CompactionWindowUnit.MINUTES);
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

    if (result.size() > 1) {
      throw new IllegalStateException(String.format(
          "Expected at most one offset row for %s[%s] but got %d",
          table, partition, result.size()));
    } else if (result.isEmpty()) {
      return new MetadataRow(NO_COMMITTED_OFFSET, -1L);
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

  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final String table,
      final int partitionKey,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    return insert.get(table)
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis));
  }

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
