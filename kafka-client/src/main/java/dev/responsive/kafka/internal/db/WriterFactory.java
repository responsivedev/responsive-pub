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

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public abstract class WriterFactory<K, P> {
  private final Logger log;
  private final String logPrefix;

  public WriterFactory(final String logPrefix) {
    this.logPrefix = logPrefix;
    this.log = new LogContext(logPrefix).logger(WriterFactory.class);
  }

  public abstract RemoteWriter<K, P> createWriter(
      final P tablePartition
  );

  public abstract String tableName();

  protected abstract P tablePartitionForKey(
      final K key
  );

  protected abstract RemoteWriteResult<P> setOffset(final long consumedOffset);

  protected abstract long offset();

  public PendingFlush beginNewFlush() {
    return new PendingFlush();
  }

  public RemoteWriteResult<P> commitPendingFlush(
      final PendingFlush pendingFlush,
      final long consumedOffset
  ) {
    return pendingFlush.completeFlush(consumedOffset);
  }

  public String failedFlushError(
      final RemoteWriteResult<P> result,
      final long consumedOffset
  ) {
    final long offset = offset();
    return String.format(
        "%sError while writing batch for table partition %s! Batch Offset: %d, Persisted Offset: %d",
        logPrefix,
        result.tablePartition(),
        consumedOffset,
        offset
    );
  }

  public class PendingFlush {
    final Map<P, RemoteWriter<K, P>> activeWriters = new HashMap<>();

    public int numRemoteWriters() {
      return activeWriters.size();
    }

    public RemoteWriter<K, P> writerForKey(
        final K key
    ) {
      final P tablePartition = tablePartitionForKey(key);
      return activeWriters.computeIfAbsent(
          tablePartition,
          WriterFactory.this::createWriter
      );
    }

    private RemoteWriteResult<P> completeFlush(final long consumedOffset) {
      final P ignored = null;
      // initialize with null as the first partial result will always fail the !isApplied check and
      // be discarded in favor of the next, and real, WriteResult in the loop below
      CompletionStage<RemoteWriteResult<P>> partialResult =
          CompletableFuture.completedStage(RemoteWriteResult.success(ignored));
      for (final RemoteWriter<K, P> writer : activeWriters.values()) {
        partialResult = partialResult.thenCombine(writer.flush(), (one, two) -> !one.wasApplied() ? one : two);
      }

      try {
        final RemoteWriteResult<P> result = partialResult.toCompletableFuture().get();
        log.debug("Succesfully flushed writes to table partition {}", result.tablePartition());
      } catch (final InterruptedException | ExecutionException e) {
        log.error("Unexpected exception while flushing to remote", e);
        throw new RuntimeException(logPrefix+ "Failed while flushing to remote", e);
      }

      // this offset is only used for recovery, so it can (and should) be done only
      // if/when all the flushes above have completed successfully
      return setOffset(consumedOffset);
    }
  }

}
