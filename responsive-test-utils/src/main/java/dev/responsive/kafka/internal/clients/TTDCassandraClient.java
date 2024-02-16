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

package dev.responsive.kafka.internal.clients;

import static dev.responsive.kafka.api.config.ResponsiveConfig.loggedConfig;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.QueryOp;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.TableCache;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.stores.TTDKeyValueTable;
import dev.responsive.kafka.internal.stores.TTDWindowedTable;
import dev.responsive.kafka.internal.utils.RemoteMonitor;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Time;

// TODO: use mock return values instead of null here and in the TTD schemas
public class TTDCassandraClient extends CassandraClient {
  private final Time time;
  private final ResponsiveStoreRegistry storeRegistry = new ResponsiveStoreRegistry();
  private final TTDMockAdmin admin;

  private final TableCache<RemoteKVTable> kvFactory;
  private final TableCache<RemoteWindowedTable> windowedFactory;

  public TTDCassandraClient(final TTDMockAdmin admin, final Time time) {
    super(loggedConfig(admin.props()));
    this.time = time;
    this.admin = admin;

    kvFactory = new TableCache<>(spec -> TTDKeyValueTable.create(spec, this));
    windowedFactory = new TableCache<>(spec -> TTDWindowedTable.create(spec, this));
  }

  public Time time() {
    return time;
  }

  public ResponsiveStoreRegistry storeRegistry() {
    return storeRegistry;
  }

  public TTDMockAdmin mockAdmin() {
    return admin;
  }

  public void advanceWallClockTime(final Duration advance) {
    flush();
    time.sleep(advance.toMillis());
  }

  private void flush() {
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
  public long count(final String tableName, final int partition) {
    final var kv = (TTDKeyValueTable) kvFactory.getTable(tableName);
    final var window = (TTDWindowedTable) windowedFactory.getTable(tableName);
    return (kv == null ? 0 : kv.count()) + (window == null ? 0 : window.count());
  }

  @Override
  public OptionalInt numPartitions(final String tableName) {
    return OptionalInt.of(1);
  }

  @Override
  public TableCache<RemoteKVTable> globalFactory() {
    return kvFactory;
  }

  @Override
  public TableCache<RemoteKVTable> kvFactory() {
    return kvFactory;
  }

  @Override
  public TableCache<RemoteKVTable> factFactory() {
    return kvFactory;
  }

  @Override
  public TableCache<RemoteWindowedTable> windowedFactory() {
    return windowedFactory;
  }
}