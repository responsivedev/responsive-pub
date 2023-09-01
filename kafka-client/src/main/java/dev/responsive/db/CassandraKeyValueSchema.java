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
import static dev.responsive.db.ColumnName.METADATA_TS;
import static dev.responsive.db.ColumnName.OFFSET;
import static dev.responsive.db.ColumnName.PARTITION_KEY;
import static dev.responsive.db.ColumnName.ROW_TYPE;
import static dev.responsive.db.ColumnName.TIMESTAMP;
import static dev.responsive.db.RowType.DATA_ROW;
import static dev.responsive.db.RowType.METADATA_ROW;
import static dev.responsive.kafka.store.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.utils.Iterators;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyValueSchema implements RemoteKeyValueSchema {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueSchema.class);

  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";

  private final CassandraClient client;

  // use ConcurrentHashMap instead of ConcurrentMap in the declaration here
  // because ConcurrentHashMap guarantees that the supplied function for
  // computeIfAbsent is invoked exactly once per invocation of the method
  // if the key is absent, else not at all. this guarantee is not present
  // in all implementations of ConcurrentMap
  private final ConcurrentHashMap<String, PreparedStatement> get;
  private final ConcurrentHashMap<String, PreparedStatement> range;
  private final ConcurrentHashMap<String, PreparedStatement> insert;
  private final ConcurrentHashMap<String, PreparedStatement> delete;
  private final ConcurrentHashMap<String, PreparedStatement> getMeta;
  private final ConcurrentHashMap<String, PreparedStatement> setOffset;

  public CassandraKeyValueSchema(final CassandraClient client) {
    this.client = client;
    get = new ConcurrentHashMap<>();
    range = new ConcurrentHashMap<>();
    insert = new ConcurrentHashMap<>();
    delete = new ConcurrentHashMap<>();
    getMeta = new ConcurrentHashMap<>();
    setOffset = new ConcurrentHashMap<>();
  }

  @Override
  public void create(final String name, Optional<Duration> ttl) {
    LOG.info("Creating data table {} in remote store.", name);
    final CreateTableWithOptions createTable = ttl.isPresent()
        ? createTable(name).withDefaultTimeToLiveSeconds(Math.toIntExact(ttl.get().getSeconds()))
        : createTable(name);

    client.execute(createTable.build());
  }

  private CreateTableWithOptions createTable(final String tableName) {
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

  @Override
  public WriterFactory<Bytes> init(
      final String table,
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    // TODO: what happens if the user has data with the key "_offset"?
    // we should consider using a special serialization format for keys
    // (e.g. adding a magic byte of 0x00 to the offset and 0x01 to all
    // th data keys) so that it's impossible for a collision to happen
    partitioner.all(kafkaPartition).forEach(sub -> {
      client.execute(
          QueryBuilder.insertInto(table)
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
    return LwtWriterFactory.reserve(this, table, partitioner, kafkaPartition);
  }

  @Override
  public MetadataRow metadata(final String table, final int partition) {
    final BoundStatement bound = getMeta.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          table, partition, result.size()));
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getLong(EPOCH.column())
      );
    }
  }

  @Override
  public BoundStatement setOffset(
      final String table,
      final int partition,
      final long offset
  ) {
    return setOffset.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public void prepare(final String tableName) {
    insert.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .insertInto(tableName)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(ROW_TYPE.column(), DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    get.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build()
    ));

    range.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThanOrEqualTo(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build()
    ));

    delete.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .deleteFrom(tableName)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));

    getMeta.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .column(EPOCH.column())
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    ));

    setOffset.computeIfAbsent(tableName, k -> client.prepare(QueryBuilder
        .update(tableName)
        .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
        .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
        .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
        .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
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
    return delete.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
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
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  @Override
  public byte[] get(final String tableName, final int partition, final Bytes key, long minValidTs) {
    final BoundStatement get = this.get.get(tableName)
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
      final String tableName,
      final int partition,
      final Bytes from,
      final Bytes to,
      long minValidTs
  ) {
    final BoundStatement range = this.range.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(from.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(to.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

    final ResultSet result = client.execute(range);
    return Iterators.kv(result.iterator(), CassandraKeyValueSchema::rows);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final String tableName,
      final int partition,
      long minValidTs) {
    final ResultSet result = client.execute(QueryBuilder
        .selectFrom(tableName)
        .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(TIMESTAMP.literal(minValidTs)))
        // since all() scans all the data anyway, allowing filtering is no worse
        .allowFiltering()
        .build()
    );

    return Iterators.kv(result.iterator(), CassandraKeyValueSchema::rows);
  }

  protected static KeyValue<Bytes, byte[]> rows(final Row row) {
    return new KeyValue<>(
        Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array()),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

}
