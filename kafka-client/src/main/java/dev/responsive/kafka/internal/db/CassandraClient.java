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

import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.utils.RemoteMonitor;
import java.time.Duration;
import java.util.Locale;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BooleanSupplier;
import org.apache.kafka.common.utils.Bytes;

/**
 * {@code CassandraClient} wraps a {@link CqlSession} with utility methods
 * specific for the Kafka Streams use case. It is expected to only work with
 * a single keyspace and that the session is already initialized to use a
 * non-default keyspace.
 */
public class CassandraClient {

  protected final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);
  private final CqlSession session;

  private final ResponsiveConfig config;
  private final TableCache<Bytes, Integer, RemoteKVTable<BoundStatement>> kvFactory;
  private final TableCache<Bytes, Integer, RemoteKVTable<BoundStatement>> factFactory;
  private final WindowedTableCache<RemoteWindowedTable<BoundStatement>> windowedFactory;
  private final TableCache<Bytes, Integer, CassandraFactTable> globalFactory;

  /**
   * @param session the Cassandra session, expected to be initialized
   *                and set to work with the proper keyspace (this class
   *                will not specify a keyspace in any CQL query)
   * @param config  the responsive configuration
   */
  public CassandraClient(final CqlSession session, final ResponsiveConfig config) {
    this.session = session;
    this.config = config;

    this.kvFactory = new TableCache<>(spec -> CassandraKeyValueTable.create(spec, this));
    this.factFactory = new TableCache<>(spec -> CassandraFactTable.create(spec, this));
    this.windowedFactory =
        new WindowedTableCache<>((spec, partitioner) -> CassandraWindowedTable.create(spec,
            this,
            partitioner
        ));
    this.globalFactory = new TableCache<>(spec -> CassandraFactTable.create(spec, this));
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

  public ResultSet execute(final String cql) {
    return session.execute(cql);
  }

  /**
   * The async version of {@link #execute(Statement)}
   */
  public CompletionStage<AsyncResultSet> executeAsync(final Statement<?> statement) {
    return session.executeAsync(statement.setIdempotent(true));
  }

  public PreparedStatement prepare(final SimpleStatement statement, final QueryOp operation) {
    switch (operation) {
      case READ:
        final String readCL = config.getString(ResponsiveConfig.READ_CONSISTENCY_LEVEL_CONFIG);
        if (readCL != null) {
          return session.prepare(statement.setConsistencyLevel(
              DefaultConsistencyLevel.valueOf(readCL.toUpperCase(Locale.ROOT)))
          );
        }
        break;
      case WRITE:
        final String writeCl = config.getString(ResponsiveConfig.WRITE_CONSISTENCY_LEVEL_CONFIG);
        if (writeCl != null) {
          return session.prepare(statement.setConsistencyLevel(
              DefaultConsistencyLevel.valueOf(writeCl.toUpperCase(Locale.ROOT)))
          );
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected query operation " + operation);
    }
    return session.prepare(statement);
  }

  /**
   * @param tableName       wait for this table to be created
   *
   * @return a monitor that multiple callers can listen on waiting for
   *         {@code tableName} to be available
   */
  public RemoteMonitor awaitTable(
      final String tableName
  ) {
    final BooleanSupplier checkRemote = () -> session.getMetadata()
        .getKeyspace(session.getKeyspace().orElseThrow())
        .flatMap(ks -> ks.getTable(tableName))
        .isPresent();
    return new RemoteMonitor(
        executor,
        checkRemote,
        Duration.ofMillis(config.getLong(ResponsiveConfig.CASSANDRA_CHECK_INTERVAL_MS))
    );
  }

  /**
   * Counts the number of elements in a remote table partition
   *
   * @param tableName      the table to count from
   * @param tablePartition the remote table partition to count
   *
   * @return the number of elements in {@code tableName} with {@code tablePartition} as
   *         the partition
   */
  public long count(final String tableName, final int tablePartition) {
    final ResultSet result = execute(QueryBuilder
        .selectFrom(tableName)
        .countAll()
        .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(tablePartition)))
        .build()
    );

    return result.one().getLong(0);
  }

  public OptionalInt numPartitions(final String tableName) {
    final ResultSet result = execute(
        String.format("SELECT DISTINCT %s FROM %s;", PARTITION_KEY.column(), tableName));

    final int numPartitions = result.all().size();
    return numPartitions == 0 ? OptionalInt.empty() : OptionalInt.of(numPartitions);
  }

  public void shutdown() {
    executor.shutdown();
    session.close();
  }

  public TableCache<Bytes, Integer, CassandraFactTable> globalFactory() {
    return globalFactory;
  }

  public TableCache<Bytes, Integer, RemoteKVTable<BoundStatement>> kvFactory() {
    return kvFactory;
  }

  public TableCache<Bytes, Integer, RemoteKVTable<BoundStatement>> factFactory() {
    return factFactory;
  }

  public WindowedTableCache<RemoteWindowedTable<BoundStatement>> windowedFactory() {
    return windowedFactory;
  }
}
