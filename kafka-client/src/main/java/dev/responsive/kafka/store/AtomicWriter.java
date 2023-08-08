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

package dev.responsive.kafka.store;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.FencingToken;
import dev.responsive.db.KeySpec;
import dev.responsive.db.RemoteSchema;
import dev.responsive.model.Result;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** {@code AtomicWriter} writes data atomically using LWTs to the remote store. */
class AtomicWriter<K> {

  private final CassandraClient client;
  private final String name;
  private final int partition;

  private final List<BatchableStatement<?>> statements;
  private final RemoteSchema<K> schema;
  private final KeySpec<K> keySpec;
  private final FencingToken fencingToken;
  private final int batchSize;

  public AtomicWriter(
      final CassandraClient client,
      final String name,
      final int partition,
      final RemoteSchema<K> remoteSchema,
      final KeySpec<K> keySpec,
      final FencingToken fencingToken,
      final int batchSize) {
    this.client = client;
    this.name = name;
    this.partition = partition;
    this.schema = remoteSchema;
    this.keySpec = keySpec;
    this.fencingToken = fencingToken;

    this.batchSize = batchSize;
    this.statements = new ArrayList<>(batchSize);
  }

  public void addAll(final Iterable<Result<K>> results) {
    results.forEach(this::add);
  }

  public void add(final Result<K> result) {
    if (result.isTombstone || keySpec.retain(result.key)) {
      statements.add(
          result.isTombstone
              ? schema.delete(name, partition, result.key)
              : schema.insert(name, partition, result.key, result.value));
    }
  }

  public CompletionStage<AtomicWriteResult> flush() {
    var result = CompletableFuture.completedStage(AtomicWriteResult.success(partition));

    final var it = statements.iterator();

    // use do-while since we always want to flush to the remote
    // store even if there's no data in the iterator - this way
    // we advance the last flushed offset (the finalized offset)
    // in the remote store
    do {
      final var builder = new BatchStatementBuilder(BatchType.UNLOGGED);
      builder.setIdempotence(true);
      fencingToken.addFencingStatement(builder, partition);

      for (int i = 0; i < batchSize && it.hasNext(); i++) {
        builder.addStatement(it.next());
      }

      result = result.thenCompose(awr -> executeAsync(builder.build()));
    } while (it.hasNext());

    statements.clear();
    return result;
  }

  private CompletionStage<AtomicWriteResult> executeAsync(final Statement<?> statement) {
    return client.executeAsync(statement).thenApply(resp -> AtomicWriteResult.of(partition, resp));
  }

  public int partition() {
    return partition;
  }

  public AtomicWriteResult setOffset(final long offset) {
    final BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.UNLOGGED);
    fencingToken.addFencingStatement(builder, partition);
    builder.addStatement(schema.setOffset(name, fencingToken, partition, offset));

    final var result = client.execute(builder.build());
    return result.wasApplied()
        ? AtomicWriteResult.success(partition)
        : AtomicWriteResult.failure(partition);
  }
}
