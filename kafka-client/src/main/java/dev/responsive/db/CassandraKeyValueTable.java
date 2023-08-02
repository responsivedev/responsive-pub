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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import dev.responsive.db.MetadataStatements.MetadataKeys;
import dev.responsive.utils.Iterators;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyValueTable implements RemoteKeyValueTable {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueTable.class);
  private static final Bytes METADATA_KEY =
      Bytes.wrap("_metadata".getBytes(StandardCharsets.UTF_8));

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
  private final MetadataStatements metadataStatements;

  public CassandraKeyValueTable(final CassandraClient client) {
    this.client = client;
    get = new ConcurrentHashMap<>();
    range = new ConcurrentHashMap<>();
    insert = new ConcurrentHashMap<>();
    delete = new ConcurrentHashMap<>();

    metadataStatements = new MetadataStatements(client, new KeyValueMetadataKeys());
  }


  @Override
  public void create(final String tableName) {
    LOG.info("Creating data table {} in remote store.", tableName);
    client.execute(SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
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
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    get.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));

    range.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThanOrEqualTo(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .build()
    ));

    delete.computeIfAbsent(tableName, k -> client.prepare(
        QueryBuilder
            .deleteFrom(tableName)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));
  }

  @Override
  public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    return Bytes.wrap(record.key());
  }

  @Override
  public Bytes bytes(final Bytes key) {
    return key;
  }

  @Override
  public CassandraClient getClient() {
    return client;
  }

  @Override
  public MetadataStatements metadata() {
    return metadataStatements;
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
    return delete.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));
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
      final Bytes key,
      final byte[] value
  ) {
    return insert.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param key       the data key
   *
   * @return the value previously set
   */
  @Override
  public byte[] get(final String tableName, final int partition, final Bytes key) {
    final BoundStatement get = this.get.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));

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

  /**
   * Retrieves a range of key value pairs from the given {@code partitionKey} and
   * {@code table} such that the keys (compared lexicographically) fall within the
   * range of {@code from} to {@code to}.
   *
   * <p>Note that the returned iterator returns values from the remote server
   * as it's iterated (data fetching is handling by the underlying Cassandra
   * session).
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   * @param from      the starting key (inclusive)
   * @param to        the ending key (exclusive)
   *
   * @return an iterator of all key-value pairs in the range
   */
  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final String tableName,
      final int partition,
      final Bytes from,
      final Bytes to
  ) {
    final BoundStatement range = this.range.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(from.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(to.get()));

    final ResultSet result = client.execute(range);
    return Iterators.kv(result.iterator(), CassandraKeyValueTable::rows);
  }

  /**
   * Retrieves all key value pairs from the given {@code partitionKey} and
   * {@code table} such that the keys are sorted lexicographically
   *
   * <p>Note that the returned iterator returns values from the remote server
   * as it's iterated (data fetching is handling by the underlying Cassandra
   * session).
   *
   * @param tableName the table to retrieve from
   * @param partition the partition
   *
   * @return an iterator of all key-value pairs
   */
  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final String tableName,
      final int partition
  ) {
    final ResultSet result = client.execute(QueryBuilder
        .selectFrom(tableName)
        .columns(DATA_KEY.column(), DATA_VALUE.column())
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .build()
    );

    return Iterators.kv(result.iterator(), CassandraKeyValueTable::rows);
  }

  protected static KeyValue<Bytes, byte[]> rows(final Row row) {
    return new KeyValue<>(
        Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array()),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

  @Override
  public int compare(final Bytes o1, final Bytes o2) {
    return Objects.compare(o1, o2, Bytes::compareTo);
  }

  // Visible for Testing
  public static class KeyValueMetadataKeys implements MetadataKeys {

    @Override
    public <T extends OngoingWhereClause<T>> T addMetaColumnsToWhere(final T builder) {
      return builder.where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)));
    }

    @Override
    public RegularInsert addKeyColumnsToInitMetadata(final RegularInsert insert) {
      return insert.value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY));
    }
  }
}
