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

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class LwtWriter<K> implements RemoteWriter<K> {

  private final CassandraClient client;
  private final Supplier<BatchableStatement<?>> fencingStatementFactory;
  private final RemoteSchema<K> schema;
  private final String tableName;
  private final int tablePartition;
  private final int batchSize;

  private final List<BatchableStatement<?>> statements;

  public LwtWriter(
      final CassandraClient client,
      final Supplier<BatchableStatement<?>> fencingStatementFactory,
      final RemoteSchema<K> schema,
      final String tableName,
      final int tablePartition,
      final int batchSize
  ) {
    this.client = client;
    this.fencingStatementFactory = fencingStatementFactory;
    this.schema = schema;
    this.tableName = tableName;
    this.tablePartition = tablePartition;
    this.batchSize = batchSize;

    statements = new ArrayList<>();
  }

  @Override
  public void insert(final K key, final byte[] value, long epochMillis) {
    statements.add(schema.insert(tableName, tablePartition, key, value, epochMillis));
  }

  @Override
  public void delete(final K key) {
    statements.add(schema.delete(tableName, tablePartition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult> flush() {
    var result = CompletableFuture.completedStage(RemoteWriteResult.success(tablePartition));

    final var it = statements.iterator();
    while (it.hasNext()) {
      final var builder = new BatchStatementBuilder(BatchType.UNLOGGED);
      builder.setIdempotence(true);
      builder.addStatement(fencingStatementFactory.get());

      for (int i = 0; i < batchSize && it.hasNext(); i++) {
        builder.addStatement(it.next());
      }

      result = result.thenCompose(awr -> executeAsync(builder.build()));
    }

    return result;
  }

  @Override
  public RemoteWriteResult setOffset(final long offset) {
    final BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.UNLOGGED);
    builder.addStatement(fencingStatementFactory.get());
    builder.addStatement(schema.setOffset(tableName, tablePartition, offset));

    final var result = client.execute(builder.build());
    return result.wasApplied()
        ? RemoteWriteResult.success(tablePartition)
        : RemoteWriteResult.failure(tablePartition);
  }

  @Override
  public int tablePartition() {
    return tablePartition;
  }

  private CompletionStage<RemoteWriteResult> executeAsync(final Statement<?> statement) {
    return client.executeAsync(statement)
        .thenApply(resp -> RemoteWriteResult.of(tablePartition, resp));
  }
}
