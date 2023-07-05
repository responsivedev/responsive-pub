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
import static dev.responsive.db.ColumnNames.OFFSET;
import static dev.responsive.db.ColumnNames.PARTITION_KEY;
import static dev.responsive.db.ColumnNames.PERMIT;
import static dev.responsive.db.ColumnNames.WINDOW_START;

import com.datastax.oss.driver.api.core.CqlSession;
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
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.RemoteMonitor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
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

  public static final Bytes OFFSET_KEY = Bytes.wrap("_offset".getBytes(StandardCharsets.UTF_8));
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

  private final ConcurrentHashMap<String, PreparedStatement> revokePermit;
  private final ConcurrentHashMap<String, PreparedStatement> acquirePermit;
  private final ConcurrentHashMap<String, PreparedStatement> finalizeTxn;

  /**
   * @param session the Cassandra session, expected to be initialized
   *                and set to work with the proper keyspace (this class
   *                will not specify a keyspace in any CQL query)
   */
  public CassandraClient(final CqlSession session) {
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

    revokePermit = new ConcurrentHashMap<>();
    acquirePermit = new ConcurrentHashMap<>();
    finalizeTxn = new ConcurrentHashMap<>();
  }

  protected CassandraClient() {
    this(null);
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
    return session.execute(statement);
  }

  /**
   * Initializes the offset entry for {@code table} by adding a
   * row with key {@link CassandraClient#OFFSET_KEY} and sets a
   * special column {@link ColumnNames#OFFSET}, which is usually
   * unset in Cassandra.
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
  public void initializeOffset(
      final String table,
      final int partitionKey
  ) {
    // TODO: what happens if the user has data with the key "_offset"?
    // we should consider using a special serialization format for keys
    // (e.g. adding a magic byte of 0x00 to the offset and 0x01 to all
    // th data keys) so that it's impossible for a collision to happen
    RegularInsert insert = QueryBuilder.insertInto(table)
        .value(PARTITION_KEY.column(), PARTITION_KEY.literal(partitionKey))
        .value(DATA_KEY.column(), DATA_KEY.literal(OFFSET_KEY))
        .value(OFFSET.column(), OFFSET.literal(-1L))
        .value(PERMIT.column(), QueryBuilder.literal(UNSET_PERMIT));

    if (windowInsert.containsKey(table)) {
      insert = insert.value(WINDOW_START.column(), WINDOW_START.literal(0L));
    }

    session.execute(insert.ifNotExists().build());
  }

  /**
   * Binds a statement to conditionally update the offset for a
   * table/partitionKey pair and additionaly revokes any ongoing
   * transaction permit.
   *
   * @param table         the table to update
   * @param partitionKey  the partition key to update
   * @param offset        the target value of the offset
   *
   * @return a {@link BoundStatement} for updating the offset
   *         for the given table/partitionKey pairing that will
   *         be applied <i>if and only if</i> the existing entry
   *         for {@code offset} has a value smaller than the supplied
   *         {@code offset} parameter.
   */
  public BoundStatement revokePermit(
      final String table,
      final int partitionKey,
      final long offset
  ) {
    return revokePermit.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(OFFSET.bind(), offset);
  }

  public BoundStatement acquirePermit(
      final String table,
      final int partitionKey,
      final UUID oldPermit,
      final UUID newPermit,
      final long offset
  ) {
    return acquirePermit.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setUuid("op", oldPermit)
        .setUuid("np", newPermit)
        .setLong(OFFSET.bind(), offset);
  }

  public BoundStatement finalizeTxn(
      final String table,
      final int partitionKey,
      final UUID permit,
      final long offset
  ) {
    return finalizeTxn.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setUuid(PERMIT.bind(), permit)
        .setLong(OFFSET.bind(), offset);
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
        .withColumn(PERMIT.column(), DataTypes.UUID)
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
        .withColumn(PERMIT.column(), DataTypes.UUID)
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

    revokePermit.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .setColumn(PERMIT.column(), QueryBuilder.literal(UNSET_PERMIT))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .build()
    ));

    acquirePermit.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(PERMIT.column(), bindMarker("np"))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .ifColumn(PERMIT.column()).isEqualTo(bindMarker("op"))
            .build()
    ));

    finalizeTxn.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(PERMIT.column(), QueryBuilder.literal(UNSET_PERMIT))
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .ifColumn(PERMIT.column()).isEqualTo(bindMarker(PERMIT.bind()))
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

    revokePermit.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .setColumn(PERMIT.column(), QueryBuilder.literal(UNSET_PERMIT))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .build()
    ));

    acquirePermit.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(PERMIT.column(), bindMarker("np"))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .ifColumn(PERMIT.column()).isEqualTo(bindMarker("op"))
            .build()
    ));

    finalizeTxn.computeIfAbsent(tableName, k -> session.prepare(
        QueryBuilder
            .update(tableName)
            .setColumn(PERMIT.column(), QueryBuilder.literal(UNSET_PERMIT))
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(OFFSET.column()).isLessThan(bindMarker(OFFSET.bind()))
            .ifColumn(PERMIT.column()).isEqualTo(bindMarker(PERMIT.bind()))
            .build()
    ));
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

    final List<Row> result = session.execute(get).all();
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

    final ResultSet result = session.execute(range);
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
    final ResultSet result = session.execute(QueryBuilder
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

    final ResultSet result = session.execute(get);
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

    final ResultSet result = session.execute(get);
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

    final ResultSet result = session.execute(get);
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

    final ResultSet result = session.execute(get);
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

    final ResultSet result = session.execute(get);
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

    final ResultSet result = session.execute(get);
    return Iterators.filterKv(
        Iterators.kv(result.iterator(), CassandraClient::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  /**
   * Gets the current (last committed) offset for the table/partition
   * pairing.
   *
   * @param tableName the table
   * @param partition the table partition
   *
   * @return the last committed offset
   */
  public OffsetRow getOffset(final String tableName, final int partition) {
    final List<Row> result = session.execute(
        QueryBuilder.selectFrom(tableName)
            .column(OFFSET.column())
            .column(PERMIT.column())
            .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(OFFSET_KEY)))
            .build()
    ).all();

    if (result.size() != 1) {
      throw new IllegalArgumentException();
    } else {
      return new OffsetRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getUuid(PERMIT.column())
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
    final ResultSet result = session.execute(QueryBuilder
        .selectFrom(tableName)
        .countAll()
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
        .build()
    );

    return result.one().getLong(0);
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
    return new RemoteMonitor(executorService, () -> session.getMetadata()
        .getKeyspace(session.getKeyspace().orElseThrow())
        .flatMap(ks -> ks.getTable(tableName))
        .isPresent());
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

  public static class OffsetRow {
    public final long offset;
    public final UUID txind;

    public OffsetRow(final long offset, final UUID txind) {
      this.offset = offset;
      this.txind = txind;
    }
  }
}
