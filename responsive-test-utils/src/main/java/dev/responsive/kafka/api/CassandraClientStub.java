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

package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.db.RemoteWindowedSchema;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaType;
import dev.responsive.utils.RemoteMonitor;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

// TODO: use mock return values instead of null here and in the TTD schemas
public class CassandraClientStub extends CassandraClient {

  private final TTDKeyValueSchema kvSchema;
  private final TTDWindowedSchema windowedSchema;

  public CassandraClientStub(final Properties props) {
    super(new ResponsiveConfig(props));
    kvSchema = new TTDKeyValueSchema(this);
    windowedSchema = new TTDWindowedSchema(this);
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
  public RemoteKeyValueSchema kvSchema(final SchemaType schemaType) {
    return kvSchema;
  }

  @Override
  public RemoteWindowedSchema windowedSchema() {
    return windowedSchema;
  }

}