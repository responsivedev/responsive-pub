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
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.RemoteKeyValueSchema;
import dev.responsive.kafka.internal.db.RemoteWindowedSchema;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.stores.TTDKeyValueSchema;
import dev.responsive.kafka.internal.stores.TTDWindowedSchema;
import dev.responsive.kafka.internal.utils.RemoteMonitor;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.common.utils.Time;

// TODO: use mock return values instead of null here and in the TTD schemas
public class TTDCassandraClient extends CassandraClient {
  private final Time time;
  private final ResponsiveStoreRegistry storeRegistry = new ResponsiveStoreRegistry();
  private final TTDMockAdmin admin;

  private final TTDKeyValueSchema kvSchema;
  private final TTDWindowedSchema windowedSchema;

  public TTDCassandraClient(final TTDMockAdmin admin, final Time time) {
    super(loggedConfig(admin.props()));
    this.time = time;
    this.admin = admin;

    kvSchema = new TTDKeyValueSchema(this);
    windowedSchema = new TTDWindowedSchema(this);
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
  public PreparedStatement prepare(final SimpleStatement statement) {
    throw new UnsupportedOperationException("Unexpected method call on TTD stub client");
  }

  @Override
  public RemoteMonitor awaitTable(
      final String tableName,
      final ScheduledExecutorService executorService
  ) {
    return new RemoteMonitor(
        executorService,
        () -> true
    );
  }

  @Override
  public long count(final String tableName, final int partition) {
    return kvSchema.count(tableName) + windowedSchema.count(tableName);
  }

  @Override
  public OptionalInt numPartitions(final String tableName) {
    return OptionalInt.of(1);
  }

  @Override
  public RemoteKeyValueSchema prepareGlobalSchema(final ResponsiveKeyValueParams params) {
    // for testing, just use the KV schema since partitions are ignored anyway,
    // which is the main difference between the stores
    return prepareKVTableSchema(params);
  }

  @Override
  public RemoteKeyValueSchema prepareKVTableSchema(final ResponsiveKeyValueParams params) {
    kvSchema.create(params.name().cassandraName(), params.timeToLive());
    kvSchema.prepare(params.name().cassandraName());
    return kvSchema;
  }

  @Override
  public RemoteWindowedSchema prepareWindowedTableSchema(final ResponsiveWindowParams params) {
    windowedSchema.create(params.name().cassandraName(), Optional.empty());
    windowedSchema.prepare(params.name().cassandraName());
    return windowedSchema;
  }

}