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

import static dev.responsive.db.CassandraClient.UNSET_PERMIT;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.db.CassandraClient;
import dev.responsive.model.Result;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * {@code AtomicWriter} writes data atomically using
 * LWTs to the remote store.
 */
class AtomicWriter<K> {

  private final CassandraClient client;
  private final String tableName;
  private final int partition;

  private final List<BatchableStatement<?>> statements = new ArrayList<>();
  private final BufferPlugin<K> plugin;
  private final long offset;
  private final UUID permit;
  private final long batchSize;

  public AtomicWriter(
      final CassandraClient client,
      final String tableName,
      final int partition,
      final BufferPlugin<K> plugin,
      final long offset,
      final UUID permit,
      final long batchSize
  ) {
    this.client = client;
    this.tableName = tableName;
    this.partition = partition;
    this.plugin = plugin;

    this.offset = offset;
    this.permit = permit;
    this.batchSize = batchSize;
  }

  public void addAll(final Iterable<Result<K>> results) {
    results.forEach(this::add);
  }

  private void add(final Result<K> result) {
    if (result.isTombstone || plugin.retain(result.key)) {
      statements.add(result.isTombstone
          ? plugin.deleteData(client, tableName, partition, result.key)
          : plugin.insertData(client, tableName, partition, result.key, result.value));
    }
  }

  public CompletionStage<AtomicWriteResult> flush() {
    boolean first = true;
    var result = CompletableFuture.completedStage(AtomicWriteResult.success(partition));

    final var it = statements.iterator();

    // use do-while since we always want to flush to the remote
    // store even if there's no data in the iterator - this way
    // we advance the last flushed offset (the finalized offset)
    // in the remote store
    do {
      final var builder = new BatchStatementBuilder(BatchType.UNLOGGED);
      builder.setIdempotence(true);
      builder.addStatement(beginBatch(first));
      first = false;

      for (int i = 0; i < batchSize && it.hasNext(); i++) {
        builder.addStatement(it.next());
      }

      // use then compose to short circuit writes to remote if any
      // of the writes fail
      result = result.thenCompose(awr -> awr.wasApplied()
          ? executeAsync(builder.build())
          : CompletableFuture.completedStage(awr)
      );
    } while (it.hasNext());

    return result;
  }

  public CompletionStage<AtomicWriteResult> finalizeTxn() {
    if (permit == null) {
      return CompletableFuture.completedStage(AtomicWriteResult.success(partition));
    }
    return executeAsync(client.finalizeTxn(tableName, partition, permit, offset));
  }

  private CompletionStage<AtomicWriteResult> executeAsync(final Statement<?> statement) {
    return client.executeAsync(statement)
        .thenApply(resp -> AtomicWriteResult.of(partition, resp));
  }

  private BatchableStatement<?> beginBatch(final boolean first) {
    return permit == null
        ? client.revokePermit(tableName, partition, offset)
        : client.acquirePermit(
            tableName,
            partition,
            first ? UNSET_PERMIT : permit,
            permit,
            offset
        );
  }
}
