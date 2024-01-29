/*
 *
 *  * Copyright 2024 Responsive Computing, Inc.
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

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class BatchFlusher<K extends Comparable<K>, P> {

  private final Logger log;
  private final KeySpec<K> keySpec;
  private final int kafkaPartition;
  private final FlushManager<K, P> flushManager;

  public BatchFlusher(
      final KeySpec<K> keySpec,
      final int kafkaPartition,
      final FlushManager<K, P> flushManager
  ) {
    this.keySpec = keySpec;
    this.kafkaPartition = kafkaPartition;
    this.flushManager = flushManager;
    this.log = new LogContext(flushManager.logPrefix()).logger(BatchFlusher.class);
  }

  public FlushResult<K, P> flushWriteBatch(
      final Map<K, Result<K>> bufferedWrites,
      final long consumedOffset
  ) {
    final var batchWriters = new BatchWriters<>(flushManager, kafkaPartition);

    prepareBatch(batchWriters, bufferedWrites, keySpec);

    final var preFlushResult = flushManager.preFlush();
    if (!preFlushResult.wasApplied()) {
      log.warn("Failed on pre-flush callback for write batch (consumedOffset={})", consumedOffset);
      return FlushResult.failed(preFlushResult, flushManager);
    }

    // the offset is only used for recovery, so it can (and should) be set only
    // if/when the entire batch of flushes has completed successfully
    final RemoteWriteResult<P> flushResult;
    try {
      flushResult = flushBatch(batchWriters).get();
    } catch (final InterruptedException | ExecutionException e) {
      log.error("Unexpected exception while flushing to remote", e);
      throw new RuntimeException("Failed while flushing batch for kafka partition "
                                     + kafkaPartition + " to remote", e);
    }
    if (!flushResult.wasApplied()) {
      log.warn("Failed to flush write batch (consumedOffset={}) on table partition {}",
               consumedOffset, flushResult.tablePartition());
      return FlushResult.failed(flushResult, flushManager);
    }

    final var postFlushResult = flushManager.postFlush(consumedOffset);
    if (!postFlushResult.wasApplied()) {
      log.warn("Failed on post-flush callback for write batch (consumedOffset={})",
               consumedOffset);
      return FlushResult.failed(postFlushResult, flushManager);
    }

    return FlushResult.success(postFlushResult, batchWriters.numTablePartitionsInBatch());
  }

  private static <K extends Comparable<K>, P> void prepareBatch(
      final BatchWriters<K, P> batchWriters,
      final Map<K, Result<K>> bufferedWrites,
      final KeySpec<K> keySpec
  ) {
    for (final Result<K> result : bufferedWrites.values()) {
      final RemoteWriter<K, P> writer = batchWriters.findOrAddWriter(result.key);
      if (result.isTombstone) {
        writer.delete(result.key);
      } else if (keySpec.retain(result.key)) {
        writer.insert(result.key, result.value, result.timestamp);
      }
    }
  }

  private static <K, P> CompletableFuture<RemoteWriteResult<P>> flushBatch(
      final BatchWriters<K, P> batchWriters
  ) {
    CompletionStage<RemoteWriteResult<P>> result =
        CompletableFuture.completedStage(RemoteWriteResult.success(null));
    for (final RemoteWriter<K, P> writer : batchWriters.allWriters()) {
      result =
          result.thenCombine(
              writer.flush(),
              (one, two) -> !one.wasApplied() ? one : two
          );
    }

    return result.toCompletableFuture();
  }

  public static class FlushResult<K, P> {
    private final RemoteWriteResult<P> result;
    private final int numTablePartitionsFlushed;
    private final FlushManager<K, P> flushManager;

    static <K, P> FlushResult<K, P> failed(
        final RemoteWriteResult<P> result,
        final FlushManager<K, P> flushManager
    ) {
      return new FlushResult<>(result, 0, flushManager);
    }

    static <K, P> FlushResult<K, P> success(
        final RemoteWriteResult<P> result,
        final int numTablePartitionsFlushed
    ) {
      return new FlushResult<>(result, numTablePartitionsFlushed, null);
    }

    private FlushResult(
        final RemoteWriteResult<P> result,
        final int numTablePartitionsFlushed,
        final FlushManager<K, P> flushManager
    ) {
      this.result = result;
      this.numTablePartitionsFlushed = numTablePartitionsFlushed;
      this.flushManager = flushManager;
    }

    public RemoteWriteResult<P> result() {
      return result;
    }

    public int numTablePartitionsFlushed() {
      return numTablePartitionsFlushed;
    }

    public String failedFlushInfo(final long consumedOffset) {
      return flushManager.failedFlushMsg(consumedOffset);
    }
  }
}
