/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A class for holding and coordinating the pending writes and other metadata involved
 * in a flush to the remote table
 */
public class WriteBatch<K extends Comparable<K>, P> {
  private final Logger log;

  private final Function<P, RemoteWriter<K, P>> writerFactory;
  private final int kafkaPartition;
  private final TablePartitioner<K, P> partitioner;

  private final Map<P, RemoteWriter<K, P>> batchWriters = new HashMap<>();
  boolean closed = false;

  public WriteBatch(
      final Function<P, RemoteWriter<K, P>> writerFactory,
      final KeySpec<K> keySpec,
      final int kafkaPartition,
      final TablePartitioner<K, P> partitioner,
      final Collection<Result<K>> bufferedWrites
    ) {
    this.log = new LogContext(String.format("[%d]", kafkaPartition)).logger(WriteBatch.class);
    this.writerFactory = writerFactory;
    this.kafkaPartition = kafkaPartition;
    this.partitioner = partitioner;

    for (final Result<K> result : bufferedWrites) {
      final RemoteWriter<K, P> writer = writerForKey(result.key);
      if (result.isTombstone) {
        writer.delete(result.key);
      } else if (keySpec.retain(result.key)) {
        writer.insert(result.key, result.value, result.timestamp);
      }
    }
  }

  private RemoteWriter<K, P> writerForKey(
      final K key
  ) {
    final P tablePartition = partitioner.tablePartition(kafkaPartition, key);
    return batchWriters.computeIfAbsent(
        tablePartition,
        writerFactory
    );
  }

  public RemoteWriteResult<P> flushBatch() {
    if (closed) {
      throw new IllegalStateException("Attempted to flush a batch that was already closed");
    }
    closed = true;

    CompletionStage<RemoteWriteResult<P>> partialResult =
        CompletableFuture.completedStage(RemoteWriteResult.success(null));
    for (final RemoteWriter<K, P> writer : batchWriters.values()) {
      partialResult =
          partialResult.thenCombine(
              writer.flush(),
              (one, two) -> !one.wasApplied() ? one : two
          );
    }

    try {
      return partialResult.toCompletableFuture().get();
    } catch (final InterruptedException | ExecutionException e) {
      log.error("Unexpected exception while flushing to remote", e);
      throw new RuntimeException("Failed while flushing batch for kafka partition " +
                                     kafkaPartition + " to remote", e);
    }
  }
}
