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

import static dev.responsive.db.ColumnName.PARTITION_KEY;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaType;
import dev.responsive.utils.RemoteMonitor;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

/**
 * {@code CassandraClient} wraps a {@link CqlSession} with utility methods
 * specific for the Kafka Streams use case. It is expected to only work with
 * a single keyspace and that the session is already initialized to use a
 * non-default keyspace.
 */
public class CassandraClient {

  private final CqlSession session;
  private final ResponsiveConfig config;
  private final RemoteKeyValueSchema kvSchema;
  private final RemoteKeyValueSchema factSchema;
  private final RemoteWindowedSchema windowedSchema;

  /**
   * @param session the Cassandra session, expected to be initialized
   *                and set to work with the proper keyspace (this class
   *                will not specify a keyspace in any CQL query)
   * @param config  the responsive configuration
   */
  public CassandraClient(final CqlSession session, final ResponsiveConfig config) {
    this.session = session;
    this.config = config;

    this.kvSchema = new CassandraKeyValueSchema(this);
    this.factSchema = new CassandraFactSchema(this);
    this.windowedSchema = new CassandraWindowedSchema(this);
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

  public PreparedStatement prepare(final SimpleStatement statement) {
    return session.prepare(statement);
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

  public OptionalInt numPartitions(final String tableName) {
    final ResultSet result = execute(
        String.format("SELECT DISTINCT %s FROM %s;", PARTITION_KEY.column(), tableName));

    final int numPartitions = result.all().size();
    return numPartitions == 0 ? OptionalInt.empty() : OptionalInt.of(numPartitions);
  }

  public RemoteKeyValueSchema kvSchema(final SchemaType schemaType) {
    switch (schemaType) {
      case KEY_VALUE: return kvSchema;
      case FACT:      return factSchema;
      default:        throw new IllegalArgumentException(schemaType.name());
    }
  }

  public RemoteWindowedSchema windowedSchema() {
    return windowedSchema;
  }
}
