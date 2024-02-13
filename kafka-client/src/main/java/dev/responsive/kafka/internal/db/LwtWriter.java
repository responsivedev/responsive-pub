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

import static dev.responsive.kafka.internal.stores.CommitBuffer.MAX_BATCH_SIZE;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class LwtWriter<K, P> implements RemoteWriter<K, P> {

  private final CassandraClient client;
  private final Supplier<BoundStatement> ensureEpoch;
  private final RemoteTable<K, BoundStatement> table;
  private final int kafkaPartition;
  private final P tablePartition;
  private final long maxBatchSize;

  private final List<BoundStatement> statements;

  public LwtWriter(
      final CassandraClient client,
      final Supplier<BoundStatement> ensureEpoch,
      final RemoteTable<K, BoundStatement> table,
      final int kafkaPartition,
      final P tablePartition
  ) {
    this(
        client,
        ensureEpoch,
        table,
        kafkaPartition,
        tablePartition,
        MAX_BATCH_SIZE
    );
  }

  @VisibleForTesting
  LwtWriter(
      final CassandraClient client,
      final Supplier<BoundStatement> ensureEpoch,
      final RemoteTable<K, BoundStatement> table,
      final int kafkaPartition,
      final P tablePartition,
      final long maxBatchSize
  ) {
    this.client = client;
    this.ensureEpoch = ensureEpoch;
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.tablePartition = tablePartition;
    this.maxBatchSize = maxBatchSize;

    statements = new ArrayList<>();
  }

  @Override
  public void insert(final K key, final byte[] value, long epochMillis) {
    statements.add(table.insert(kafkaPartition, key, value, epochMillis));
  }

  @Override
  public void delete(final K key) {
    statements.add(table.delete(kafkaPartition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult<P>> flush() {
    CompletionStage<RemoteWriteResult<P>> result =
        CompletableFuture.completedStage(RemoteWriteResult.success(tablePartition));

    final var it = statements.iterator();
    while (it.hasNext()) {
      final var builder = new BatchStatementBuilder(BatchType.UNLOGGED);
      builder.setIdempotence(true);
      builder.addStatement(ensureEpoch.get());

      for (int i = 0; i < maxBatchSize && it.hasNext(); i++) {
        builder.addStatement(it.next());
      }

      result = result.thenCompose(awr -> executeAsync(builder.build()));
    }

    return result;
  }

  private CompletionStage<RemoteWriteResult<P>> executeAsync(final Statement<?> statement) {
    return client.executeAsync(statement)
        .thenApply(resp -> RemoteWriteResult.of(tablePartition, resp));
  }
}
