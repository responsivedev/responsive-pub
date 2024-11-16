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
import static dev.responsive.kafka.internal.db.ColumnName.TTL_SECONDS;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraFactTable implements RemoteKVTable<BoundStatement> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CassandraFactTable.class);

  private final String name;
  private final CassandraClient client;
  private final Optional<TtlResolver<?, ?>> ttlResolver;

  private final PreparedStatement get;
  private final PreparedStatement getWithTimestamp;
  private final PreparedStatement insert;
  private final PreparedStatement insertWithTtl;
  private final PreparedStatement delete;
  private final PreparedStatement fetchOffset;
  private final PreparedStatement setOffset;

  public CassandraFactTable(
      final String name,
      final CassandraClient client,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final PreparedStatement get,
      final PreparedStatement getWithTimestamp,
      final PreparedStatement insert,
      final PreparedStatement insertWithTtl,
      final PreparedStatement delete,
      final PreparedStatement fetchOffset,
      final PreparedStatement setOffset
  ) {
    this.name = name;
    this.client = client;
    this.ttlResolver = ttlResolver;
    this.get = get;
    this.getWithTimestamp = getWithTimestamp;
    this.insert = insert;
    this.insertWithTtl = insertWithTtl;
    this.delete = delete;
    this.fetchOffset = fetchOffset;
    this.setOffset = setOffset;
  }

  public static CassandraFactTable create(
      final RemoteTableSpec spec,
      final CassandraClient client
  ) {
    final String name = spec.tableName();
    final var ttlResolver = spec.ttlResolver();
    LOG.info("Creating fact data table {} in remote store.", name);

    final CreateTableWithOptions createTable = spec.applyDefaultOptions(
        createTable(name, ttlResolver)
    );

    // separate metadata from the main table for the fact schema, this is acceptable
    // because we don't use the metadata at all for fencing operations and writes to
    // it do not need to be atomic (transactional with the original table). we cannot
    // effectively use the same table (as we do with the normal KeyValueSchema) because
    // TWCS cannot properly compact files if there are any overwrites, which there are
    // for the metadata columns
    final CreateTableWithOptions createMetadataTable = SchemaBuilder
        .createTable(metadataTable(name))
        .ifNotExists()
        .withPartitionKey(ROW_TYPE.column(), DataTypes.TINYINT)
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withColumn(OFFSET.column(), DataTypes.BIGINT);

    client.execute(createTable.build());
    client.execute(createMetadataTable.build());

    final var insert = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(ROW_TYPE.column(), RowType.DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build(),
        QueryOp.WRITE
    );

    final var insertWithTtl = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(ROW_TYPE.column(), RowType.DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .usingTtl(bindMarker(TTL_SECONDS.bind()))
            .build(),
        QueryOp.WRITE
    );

    final var get = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_VALUE.column())
            .where(ROW_TYPE.relation().isEqualTo(RowType.DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition (it actually  only
            // returns a single value)
            .allowFiltering()
            .build(),
        QueryOp.READ
    );

    final var getWithTimestamp = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_VALUE.column(), TIMESTAMP.column())
            .where(ROW_TYPE.relation().isEqualTo(RowType.DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition (it actually  only
            // returns a single value)
            .allowFiltering()
            .build(),
        QueryOp.READ
    );

    final var delete = client.prepare(
        QueryBuilder
            .deleteFrom(name)
            .where(ROW_TYPE.relation().isEqualTo(RowType.DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build(),
        QueryOp.WRITE
    );

    final var fetchOffset = client.prepare(
        QueryBuilder
            .selectFrom(metadataTable(name))
            .column(OFFSET.column())
            .where(ROW_TYPE.relation().isEqualTo(RowType.METADATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .build(),
        QueryOp.READ
    );

    final var setOffset = client.prepare(
        QueryBuilder
            .update(metadataTable(name))
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(ROW_TYPE.relation().isEqualTo(RowType.METADATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .build(),
        QueryOp.WRITE
    );

    return new CassandraFactTable(
        name,
        client,
        ttlResolver,
        get,
        getWithTimestamp,
        insert,
        insertWithTtl,
        delete,
        fetchOffset,
        setOffset
    );
  }

  private static CreateTableWithOptions createTable(
      final String tableName,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) {
    final var baseOptions = SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(ROW_TYPE.column(), DataTypes.TINYINT)
        .withPartitionKey(DATA_KEY.column(), DataTypes.BLOB)
        .withColumn(TIMESTAMP.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB);

    if (ttlResolver.isPresent() && ttlResolver.get().defaultTtl().isFinite()) {
      return baseOptions.withDefaultTimeToLiveSeconds(
          (int) ttlResolver.get().defaultTtl().toSeconds());
    } else {
      return baseOptions;
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CassandraFactFlushManager init(
      final int kafkaPartition
  ) {
    client.execute(
        QueryBuilder.insertInto(metadataTable(name))
            .value(ROW_TYPE.column(), RowType.METADATA_ROW.literal())
            .value(PARTITION_KEY.column(), PARTITION_KEY.literal(kafkaPartition))
            .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
            .ifNotExists()
            .build()
    );

    return new CassandraFactFlushManager(this, client, kafkaPartition);
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final BoundStatement bound = fetchOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), kafkaPartition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() > 1) {
      throw new IllegalStateException(String.format(
          "Expected at most one offset row for %s[%s] but got %d",
          name, kafkaPartition, result.size()));
    } else if (result.isEmpty()) {
      return NO_COMMITTED_OFFSET;
    } else {
      final long offset = result.get(0).getLong(OFFSET.column());
      LOG.info("Got offset for {}[{}]: {}", name, kafkaPartition, offset);
      return offset;
    }
  }

  public BoundStatement setOffset(
      final int kafkaPartition,
      final long offset
  ) {
    LOG.info("Setting offset in metadata table {} for {}[{}] to {}",
             metadataTable(name), name, kafkaPartition, offset);
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), kafkaPartition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    throw new UnsupportedOperationException(
        "approximateNumEntries is not supported on fact tables");
  }

  @Override
  @CheckReturnValue
  public BoundStatement delete(
      final int kafkaPartition,
      final Bytes key
  ) {
    return delete
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));
  }

  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    if (ttlResolver.isPresent()) {
      final Optional<TtlDuration> rowTtl =
          ttlResolver.get().computeInsertTtl(key, value, epochMillis);

      // If user happens to return same ttl value as the default, skip applying it at
      // the row level since this is less efficient in Scylla
      if (rowTtl.isPresent() && !rowTtl.get().equals(ttlResolver.get().defaultTtl())) {

        // You can set the row ttl to 0 in Scylla to apply no/infinite ttl
        final int rowTtlOverrideSeconds = rowTtl.get().isFinite()
            ? (int) rowTtl.get().toSeconds()
            : 0;

        return insertWithTtl
            .bind()
            .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
            .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value))
            .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis))
            .setInt(TTL_SECONDS.bind(), rowTtlOverrideSeconds);
      }
    }

    return insert
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis));
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, long streamTimeMs) {
    if (ttlResolver.isEmpty()) {
      return simpleGet(key);
    } else if (ttlResolver.get().needsValueToComputeTtl()) {
      return postFilterGet(key, streamTimeMs);
    } else {
      final TtlDuration ttl = ttlResolver.get().resolveRowTtl(key, null);
      if (ttl.isFinite()) {
        final long minValidTimeMs = streamTimeMs - ttl.toMillis();
        return preFilterGet(key, minValidTimeMs);
      } else {
        return simpleGet(key);
      }
    }
  }

  /**
   * Simple "get" with no filtering for when ttl is infinite or there is no ttl at all
   */
  private byte[] simpleGet(final Bytes key) {
    // Just delegate to the preFilterGet with a min valid timestamp of -1
    // since this should not exclude anything since it's not worth having
    // a third "get" PreparedStatement without the gte(timestamp) filter
    return preFilterGet(key, -1L);
  }

  /**
   * Simple "get" with server-side filtering on ttl. Used when ttl is possible
   * to compute based on the key alone, is default only, or has no ttl at all
   */
  private byte[] preFilterGet(final Bytes key, final long minValidTimeMs) {
    final BoundStatement getQuery = get
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTimeMs));

    final List<Row> result = client.execute(getQuery).all();

    if (result.size() > 1) {
      throw new IllegalStateException("Received multiple results for the same key");
    } else if (result.isEmpty()) {
      return null;
    } else {
      return getValueFromRow(result.get(0));
    }
  }

  private byte[] postFilterGet(final Bytes key, long streamTimeMs) {
    final BoundStatement getQuery = getWithTimestamp
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));

    final List<Row> result = client.execute(getQuery).all();

    if (result.size() > 1) {
      throw new IllegalStateException("Received multiple results for the same key");
    } else if (result.isEmpty()) {
      return null;
    }

    final Row rowResult = result.get(0);
    final byte[] value = getValueFromRow(rowResult);
    final TtlDuration ttl = ttlResolver.get().resolveRowTtl(key, value);

    if (ttl.isFinite()) {
      final long minValidTsFromValue = streamTimeMs - ttl.toMillis();
      final long recordTs = rowResult.getInstant(TIMESTAMP.column()).toEpochMilli();
      if (recordTs < minValidTsFromValue) {
        return null;
      }
    }

    return value;
  }

  private byte[] getValueFromRow(final Row row) {
    return Objects.requireNonNull(row.getByteBuffer(DATA_VALUE.column())).array();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      long streamTimeMs
  ) {
    throw new UnsupportedOperationException("range scans are not supported on fact tables.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final int kafkaPartition,
      long streamTimeMs
  ) {
    throw new UnsupportedOperationException("all is not supported on fact tables");
  }

  private static String metadataTable(final String tableName) {
    return tableName + "_md";
  }

}
