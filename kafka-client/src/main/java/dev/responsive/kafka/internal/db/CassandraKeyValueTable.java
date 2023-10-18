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
import static dev.responsive.kafka.internal.db.ColumnName.METADATA_TS;
import static dev.responsive.kafka.internal.db.ColumnName.OFFSET;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.ROW_TYPE;
import static dev.responsive.kafka.internal.db.ColumnName.TIMESTAMP;
import static dev.responsive.kafka.internal.db.RowType.DATA_ROW;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.utils.Iterators;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyValueTable implements RemoteKVTable<BoundStatement> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueTable.class);
  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";

  private final String name;
  private final CassandraClient client;

  private final PreparedStatement get;
  private final PreparedStatement range;
  private final PreparedStatement insert;
  private final PreparedStatement delete;
  private final PreparedStatement getMeta;
  private final PreparedStatement setOffset;

  public static CassandraKeyValueTable create(
      final CassandraTableSpec spec,
      final CassandraClient client
  ) throws InterruptedException, TimeoutException {
    final String name = spec.tableName();
    LOG.info("Creating data table {} in remote store.", name);
    client.execute(spec.applyOptions(createTable(name)).build());

    client.awaitTable(name).await(Duration.ofSeconds(60));

    final var insert = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(ROW_TYPE.column(), DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    );

    final var get = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build()
    );

    final var range = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThanOrEqualTo(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build()
    );

    final var delete = client.prepare(
        QueryBuilder
            .deleteFrom(name)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    );

    final var getMeta = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(EPOCH.column())
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    );

    final var setOffset = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    );

    return new CassandraKeyValueTable(
        name,
        client,
        get,
        range,
        insert,
        delete,
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
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
        .withColumn(OFFSET.column(), DataTypes.BIGINT)
        .withColumn(EPOCH.column(), DataTypes.BIGINT)
        .withColumn(TIMESTAMP.column(), DataTypes.TIMESTAMP);
  }

  // Visible for Testing
  public CassandraKeyValueTable(
      final String name,
      final CassandraClient client,
      final PreparedStatement get,
      final PreparedStatement range,
      final PreparedStatement insert,
      final PreparedStatement delete,
      final PreparedStatement getMeta,
      final PreparedStatement setOffset
  ) {
    this.name = name;
    this.client = client;
    this.get = get;
    this.range = range;
    this.insert = insert;
    this.delete = delete;
    this.getMeta = getMeta;
    this.setOffset = setOffset;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<Bytes> init(
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    partitioner.all(kafkaPartition).forEach(sub -> {
      client.execute(
          QueryBuilder.insertInto(name)
              .value(PARTITION_KEY.column(), PARTITION_KEY.literal(sub))
              .value(ROW_TYPE.column(), METADATA_ROW.literal())
              .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
              .value(TIMESTAMP.column(), TIMESTAMP.literal(METADATA_TS))
              .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
              .value(EPOCH.column(), EPOCH.literal(0L))
              .ifNotExists()
              .build()
      );
    });
    return LwtWriterFactory.reserve(this, client, partitioner, kafkaPartition);
  }

  @Override
  public byte[] get(
      final int partition,
      final Bytes key,
      final long minValidTs
  ) {
    final BoundStatement get = this.get
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

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

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int partition,
      final Bytes from,
      final Bytes to,
      final long minValidTs
  ) {
    final BoundStatement range = this.range
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(from.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(to.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

    final ResultSet result = client.execute(range);
    return Iterators.kv(result.iterator(), CassandraKeyValueTable::rows);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final int partition,
      final long minValidTs
  ) {
    final ResultSet result = client.execute(QueryBuilder
        .selectFrom(name)
        .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(TIMESTAMP.literal(minValidTs)))
        // since all() scans all the data anyway, allowing filtering is no worse
        .allowFiltering()
        .build()
    );

    return Iterators.kv(result.iterator(), CassandraKeyValueTable::rows);
  }

  @Override
  public BoundStatement insert(
      final int partitionKey,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    return insert
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  @Override
  public BoundStatement delete(
      final int partitionKey,
      final Bytes key
  ) {
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));
  }

  @Override
  public MetadataRow metadata(final int partition) {
    final BoundStatement bound = getMeta
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() > 1) {
      throw new IllegalStateException(String.format(
          "Expected at most one offset row for %s[%s] but got %d",
          name, partition, result.size()));
    } else if (result.isEmpty()) {
      return new MetadataRow(NO_COMMITTED_OFFSET, -1L);
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getLong(EPOCH.column())
      );
    }
  }

  @Override
  public BoundStatement setOffset(final int partition, final long offset) {
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public long approximateNumEntries(final int partition) {
    return client.count(name(), partition);
  }

  private static KeyValue<Bytes, byte[]> rows(final Row row) {
    return new KeyValue<>(
        Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array()),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }
}