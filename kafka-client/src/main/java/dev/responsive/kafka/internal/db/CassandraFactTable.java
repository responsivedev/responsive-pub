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
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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

  private final PreparedStatement get;
  private final PreparedStatement insert;
  private final PreparedStatement delete;
  private final PreparedStatement fetchOffset;
  private final PreparedStatement setOffset;

  public CassandraFactTable(
      final String name,
      final CassandraClient client,
      final PreparedStatement get,
      final PreparedStatement insert,
      final PreparedStatement delete,
      final PreparedStatement fetchOffset,
      final PreparedStatement setOffset
  ) {
    this.name = name;
    this.client = client;
    this.get = get;
    this.insert = insert;
    this.delete = delete;
    this.fetchOffset = fetchOffset;
    this.setOffset = setOffset;
  }

  public static CassandraFactTable create(
      final RemoteTableSpec spec,
      final CassandraClient client
  ) {
    final String name = spec.tableName();
    LOG.info("Creating fact data table {} in remote store.", name);

    final CreateTableWithOptions createTable = spec.applyOptions(createTable(name));

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
        get,
        insert,
        delete,
        fetchOffset,
        setOffset
    );
  }

  private static CreateTableWithOptions createTable(final String tableName) {
    return SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(ROW_TYPE.column(), DataTypes.TINYINT)
        .withPartitionKey(DATA_KEY.column(), DataTypes.BLOB)
        .withColumn(TIMESTAMP.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB);
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
    return insert
        .bind()
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis));
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, long minValidTs) {
    final BoundStatement get = this.get
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
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      long minValidTs) {
    throw new UnsupportedOperationException("range scans are not supported on fact tables.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final int kafkaPartition,
      long minValidTs) {
    throw new UnsupportedOperationException("all is not supported on fact tables");
  }

  private static String metadataTable(final String tableName) {
    return tableName + "_md";
  }

}
