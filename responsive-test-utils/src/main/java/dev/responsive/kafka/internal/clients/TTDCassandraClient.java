/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.clients;

import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.QueryOp;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.TTDKeyValueTable;
import dev.responsive.kafka.internal.db.TTDWindowTable;
import dev.responsive.kafka.internal.db.TableCache;
import dev.responsive.kafka.internal.db.WindowedTableCache;
import dev.responsive.kafka.internal.db.inmemory.InMemoryKVTable;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.RemoteMonitor;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Time;

public class TTDCassandraClient extends CassandraClient {
  private final ResponsiveStoreRegistry storeRegistry = new ResponsiveStoreRegistry();
  private final TTDMockAdmin admin;
  private final Time time;

  private final TableCache<RemoteKVTable<BoundStatement>> kvFactory;
  private final WindowedTableCache<RemoteWindowTable<BoundStatement>> windowedFactory;

  public TTDCassandraClient(final TTDMockAdmin admin, final Time time) {
    super(loggedConfig(admin.props()));
    this.admin = admin;
    this.time = time;

    kvFactory = new TableCache<>(spec -> new TTDKeyValueTable(spec, this));
    windowedFactory = new WindowedTableCache<>(
        (spec, partitioner) -> TTDWindowTable.create(spec, this, partitioner));
  }

  public ResponsiveStoreRegistry storeRegistry() {
    return storeRegistry;
  }

  public TTDMockAdmin mockAdmin() {
    return admin;
  }

  public long currentWallClockTimeMs() {
    return time.milliseconds();
  }

  public void advanceWallClockTime(final Duration advance) {
    flush();
    time.sleep(advance.toMillis());
  }

  public void flush() {
    storeRegistry.stores().forEach(s -> s.onCommit().accept(0L));
  }

  @Override
  public ResultSet execute(final Statement<?> statement) {
    return null;
  }

  @Override
  public ResultSet execute(final String cql) {
    return null;
  }

  @Override
  public CompletionStage<AsyncResultSet> executeAsync(final Statement<?> statement) {
    throw new UnsupportedOperationException("Unexpected method call on TTD stub client");
  }

  @Override
  public PreparedStatement prepare(final SimpleStatement statement, final QueryOp operation) {
    throw new UnsupportedOperationException("Unexpected method call on TTD stub client");
  }

  @Override
  public RemoteMonitor awaitTable(
      final String tableName
  ) {
    return new RemoteMonitor(executor, () -> true);
  }

  @Override
  public long count(final String tableName, final int tablePartition) {
    final var kv = (InMemoryKVTable) kvFactory.getTable(tableName);
    final var window = (TTDWindowTable) windowedFactory.getTable(tableName);
    return (kv == null ? 0 : kv.approximateNumEntries(tablePartition))
        + (window == null ? 0 : window.count());
  }

  @Override
  public OptionalInt numPartitions(final String tableName) {
    return OptionalInt.of(1);
  }

  @Override
  public TableCache<RemoteKVTable<BoundStatement>> kvFactory() {
    return kvFactory;
  }

  @Override
  public TableCache<RemoteKVTable<BoundStatement>> factFactory() {
    return kvFactory;
  }

  @Override
  public WindowedTableCache<RemoteWindowTable<BoundStatement>> windowedFactory() {
    return windowedFactory;
  }
}