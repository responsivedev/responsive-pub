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

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.RemoteMonitor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code CassandraClient} wraps a {@link CqlSession} with utility methods
 * specific for the Kafka Streams use case. It is expected to only work with
 * a single keyspace and that the session is already initialized to use a
 * non-default keyspace.
 */
public class CassandraClient {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);

  public static final Bytes METADATA_KEY = Bytes.wrap("_metadata".getBytes(StandardCharsets.UTF_8));
  public static final UUID UNSET_PERMIT = new UUID(0, 0);

  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";
  private static final String W_FROM_BIND = "wf";
  private static final String W_TO_BIND = "wt";

  private final CqlSession session;

  // use ConcurrentHashMap instead of ConcurrentMap in the declaration here
  // because ConcurrentHashMap guarantees that the supplied function for
  // computeIfAbsent is invoked exactly once per invocation of the method
  // if the key is absent, else not at all. this guarantee is not present
  // in all implementations of ConcurrentMap
  private final ConcurrentHashMap<String, PreparedStatement> tableGets;
  private final ConcurrentHashMap<String, PreparedStatement> tableRange;
  private final ConcurrentHashMap<String, PreparedStatement> tableInserts;
  private final ConcurrentHashMap<String, PreparedStatement> tableDeletes;

  private final ConcurrentHashMap<String, PreparedStatement> windowInsert;
  private final ConcurrentHashMap<String, PreparedStatement> windowFetch;
  private final ConcurrentHashMap<String, PreparedStatement> windowFetchAll;
  private final ConcurrentHashMap<String, PreparedStatement> windowFetchRange;
  private final ConcurrentHashMap<String, PreparedStatement> windowBackFetch;
  private final ConcurrentHashMap<String, PreparedStatement> windowBackFetchAll;
  private final ConcurrentHashMap<String, PreparedStatement> windowBackFetchRange;

  private final ConcurrentHashMap<String, PreparedStatement> getMetadata;
  private final ConcurrentHashMap<String, PreparedStatement> reserveEpoch;
  private final ConcurrentHashMap<String, PreparedStatement> ensureEpoch;
  private final ConcurrentHashMap<String, PreparedStatement> setOffset;
  private ResponsiveConfig config;

  /**
   * @param session the Cassandra session, expected to be initialized
   *                and set to work with the proper keyspace (this class
   *                will not specify a keyspace in any CQL query)
   * @param config  the responsive configuration
   */
  public CassandraClient(final CqlSession session, final ResponsiveConfig config) {
    this.session = session;

    tableGets = new ConcurrentHashMap<>();
    tableRange = new ConcurrentHashMap<>();
    tableInserts = new ConcurrentHashMap<>();
    tableDeletes = new ConcurrentHashMap<>();

    windowInsert = new ConcurrentHashMap<>();
    windowFetch = new ConcurrentHashMap<>();
    windowFetchAll = new ConcurrentHashMap<>();
    windowFetchRange = new ConcurrentHashMap<>();
    windowBackFetch = new ConcurrentHashMap<>();
    windowBackFetchAll = new ConcurrentHashMap<>();
    windowBackFetchRange = new ConcurrentHashMap<>();

    getMetadata = new ConcurrentHashMap<>();
    reserveEpoch = new ConcurrentHashMap<>();
    ensureEpoch = new ConcurrentHashMap<>();
    setOffset = new ConcurrentHashMap<>();

    this.config = config;
  }

  protected CassandraClient(final ResponsiveConfig config) {
    this(null, config);
  }

  /**
   * Executes an arbitrary statement directly with Cassandra. It
   * is preferred to use any of the strongly typed methods if
   * possible.
   *
   * @param statement the statement to execute
   * @return the result returned from Cassandra
   */
  public ResultSet execute(final Statement<?> statement) {
    return session.execute(statement.setIdempotent(true));
  }

  /**
   * The async version of {@link #execute(Statement)}
   */
  public CompletionStage<AsyncResultSet> executeAsync(final Statement<?> statement) {
    return session.executeAsync(statement.setIdempotent(true));
  }

  /**
   * Initializes the metadata entry for {@code table} by adding a
   * row with key {@link CassandraClient#METADATA_KEY} and sets
   * special columns {@link ColumnNames#OFFSET} and {@link ColumnNames#EPOCH}.
   *
   * <p>The {@code partitionKey} is the unit of atomicity for the
   * offset compare-and-check operation. Two operations that run
   * concurrently for different {@code partitionKey} values will
   * not be fenced.
   *
   * <p>Note that this method is idempotent as it uses Cassandra's
   * {@code IF NOT EXISTS} functionality.
   *
   * @param table         the table that is initialized
   * @param partitionKey  the partition to initialize
   */
  public void initializeMetadata(
      final String table,
      final int partitionKey
  ) {
    // TODO: what happens if the user has data with the key "_offset"?
    // we should consider using a special serialization format for keys
    // (e.g. adding a magic byte of 0x00 to the offset and 0x01 to all
    // th data keys) so that it's impossible for a collision to happen
    RegularInsert insert = QueryBuilder.insertInto(table)
        .value(PARTITION_KEY.column(), PARTITION_KEY.literal(partitionKey))
        .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
        .value(OFFSET.column(), OFFSET.literal(-1L))
        .value(EPOCH.column(), QueryBuilder.literal(0L));

    if (windowInsert.containsKey(table)) {
      insert = insert.value(WINDOW_START.column(), WINDOW_START.literal(0L));
    }

    session.execute(insert.ifNotExists().build());
  }


  /**
   * Creates a new table to store changelog data with the following
   * schema:
   *
   * <pre>
   *   ((partitionKey INT), dataKey BLOB), dataValue BLOB, offset LONG
   * </pre>
   *
   * @param tableName the name of the table
   */
  public void createDataTable(final String tableName) {
    LOG.info("Creating data table {} in remote store.", tableName);
    session.execute(SchemaBuilder
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

  /**
   * Creates a new table to store changelog data with the following
   * schema:
   *
   * <pre>
   *   ((partitionKey INT), dataKey BLOB, windowStart TIMESTAMP), dataValue BLOB, offset LONG
   * </pre>
   *
   * @param tableName         the name of the table
   */
  public void createWindowedDataTable(final String tableName) {
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
    session.execute(SchemaBuilder
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

  /**
   * Prepares statements for inserting and retrieving data from
   * {@code tableName}. This method is idempotent and will only
   * prepare the statements once per table, even if called from
   * multiple threads concurrently.
   *
   * @param tableName the table to prepare
   */
  public void prepareStatements(final String tableName) {
    tableInserts.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .insertInto(tableName)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    tableGets.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));

    tableRange.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThanOrEqualTo(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .build()
    ));

    tableDeletes.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .deleteFrom(tableName)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build()
    ));

    getMetadata.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .column(EPOCH.column())
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    ));

    setOffset.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));

    reserveEpoch.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isLessThan(bindMarker(EPOCH.bind()))
            .build()
    ));

    ensureEpoch.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));
  }

  /**
   * Prepares the Cassandra queries for windowed tables.
   *
   * @see #prepareStatements(String)
   */
  public void prepareWindowedStatements(final String tableName) {
    windowInsert.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .insertInto(tableName)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(WINDOW_START.column(), bindMarker(WINDOW_START.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    ));

    windowFetch.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(W_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(W_TO_BIND)))
            .build()
    ));

    windowFetchAll.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .build()
    ));

    windowFetchRange.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(TO_BIND)))
            .build()
    ));

    windowBackFetch.computeIfAbsent(tableName, k -> session.prepare(
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

    windowBackFetchAll.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    ));

    windowBackFetchRange.computeIfAbsent(tableName, k -> session.prepare(
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

    getMetadata.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .selectFrom(tableName)
            .column(OFFSET.column())
            .column(EPOCH.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .build()
    ));

    setOffset.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));

    reserveEpoch.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(EPOCH.column()).isLessThan(bindMarker(EPOCH.bind()))
            .build()
    ));

    ensureEpoch.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));
  }

  @CheckReturnValue
  public BoundStatement reserveEpoch(
      final String table,
      final int partitionKey,
      final long epoch
  ) {
    return reserveEpoch.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(EPOCH.bind(), epoch);
  }

  @CheckReturnValue
  public BoundStatement ensureEpoch(
      final String table,
      final int partitionKey,
      final long epoch
  ) {
    return ensureEpoch.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(EPOCH.bind(), epoch);
  }

  @CheckReturnValue
  public BoundStatement setOffset(
      final String table,
      final int partitionKey,
      final long offset,
      final long epoch
  ) {
    return setOffset.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(OFFSET.bind(), offset)
        .setLong(EPOCH.bind(), epoch);
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
  @CheckReturnValue
  public BoundStatement deleteData(
      final String table,
      final int partitionKey,
      final Bytes key
  ) {
    return tableDeletes.get(table)
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
  @CheckReturnValue
  public BoundStatement insertData(
      final String table,
      final int partitionKey,
      final Bytes key,
      final byte[] value
  ) {
    return tableInserts.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
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
  @CheckReturnValue
  public BoundStatement insertWindowed(
      final String table,
      final int partitionKey,
      final Bytes key,
      final long windowStart,
      final byte[] value
  ) {
    return windowInsert.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(windowStart))
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
  public byte[] get(final String tableName, final int partition, final Bytes key) {
    final BoundStatement get = tableGets.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));

    final List<Row> result = execute(get).all();
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
  public KeyValueIterator<Bytes, byte[]> range(
      final String tableName,
      final int partition,
      final Bytes from,
      final Bytes to
  ) {
    final BoundStatement range = tableRange.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(from.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(to.get()));

    final ResultSet result = execute(range);
    return Iterators.kv(result.iterator(), CassandraClient::rows);
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
  public KeyValueIterator<Bytes, byte[]> all(
      final String tableName,
      final int partition
  ) {
    final ResultSet result = execute(QueryBuilder
        .selectFrom(tableName)
        .columns(DATA_KEY.column(), DATA_VALUE.column())
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .build()
    );

    return Iterators.kv(result.iterator(), CassandraClient::rows);
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowFetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = execute(get);
    return Iterators.kv(
        result.iterator(),
        CassandraClient::windowRows
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowBackFetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(W_FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(W_TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = execute(get);
    return Iterators.kv(
        result.iterator(),
        CassandraClient::windowRows
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowFetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraClient::windowRows),
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowBackFetch.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setByteBuffer(FROM_BIND, ByteBuffer.wrap(fromKey.get()))
        .setByteBuffer(TO_BIND, ByteBuffer.wrap(toKey.get()));

    final ResultSet result = execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraClient::windowRows),
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowFetchAll.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition)
        .setInstant(FROM_BIND, Instant.ofEpochMilli(timeFrom))
        .setInstant(TO_BIND, Instant.ofEpochMilli(timeTo));

    final ResultSet result = execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraClient::windowRows),
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    final BoundStatement get = windowBackFetchAll.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);

    final ResultSet result = execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraClient::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  /**
   * Get the metadata row for the table/partition pairing which includes:
   * <ol>
   *   <li>The current (last committed) offset</li>
   *   <li>The current writer epoch</li>
   * </ol>
   *
   * @param tableName the table
   * @param partition the table partition
   *
   * @return the metadata row
   */
  public MetadataRow metadata(final String tableName, final int partition) {
    final List<Row> result = execute(
        getMetadata.get(tableName)
            .bind()
            .setInt(PARTITION_KEY.bind(), partition)
    ).all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          tableName, partition, result.size()));
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getLong(EPOCH.column())
      );
    }
  }

  /**
   * Counts the number of elements in a partition
   *
   * @param tableName the table to count from
   * @param partition the partition to count
   *
   * @return the number of elements in {@code tableName} with {@code partition} as
   *         the partition
   */
  public long count(final String tableName, final int partition) {
    final ResultSet result = execute(QueryBuilder
        .selectFrom(tableName)
        .countAll()
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .build()
    );

    return result.one().getLong(0);
  }

  /**
   * @param tableName the table to query
   * @return the number of partitions for this table, or empty if the table has not been
   *         initialized
   */
  public OptionalInt numPartitions(final String tableName) {
    final ResultSet result = session.execute(
        String.format("SELECT DISTINCT %s FROM %s;", PARTITION_KEY.column(), tableName));
    final int numPartitions = result.all().size();
    return numPartitions == 0 ? OptionalInt.empty() : OptionalInt.of(numPartitions);
  }

  /**
   * @param tableName       wait for this table to be created
   * @param executorService the executor service to use for waiting
   *
   * @return a monitor that multiple callers can listen on waiting for
   *         {@code tableName} to be available
   */
  public RemoteMonitor awaitTable(
      final String tableName,
      final ScheduledExecutorService executorService
  ) {
    final BooleanSupplier checkRemote = () -> session.getMetadata()
        .getKeyspace(session.getKeyspace().orElseThrow())
        .flatMap(ks -> ks.getTable(tableName))
        .isPresent();
    return new RemoteMonitor(
        executorService,
        checkRemote,
        Duration.ofMillis(config.getLong(ResponsiveConfig.REMOTE_TABLE_CHECK_INTERVAL_MS_CONFIG))
    );
  }

  private static KeyValue<Bytes, byte[]> rows(final Row row) {
    return new KeyValue<>(
        Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array()),
        row.getByteBuffer(DATA_VALUE.column()).array()
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

  public static class MetadataRow {
    public final long offset;
    public final long epoch;

    public MetadataRow(final long offset, final long epoch) {
      this.offset = offset;
      this.epoch = epoch;
    }

    @Override
    public String toString() {
      return "MetadataRow{"
          + "offset=" + offset
          + ", epoch=" + epoch
          + '}';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final MetadataRow that = (MetadataRow) o;
      return offset == that.offset && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(offset, epoch);
    }
  }
}
