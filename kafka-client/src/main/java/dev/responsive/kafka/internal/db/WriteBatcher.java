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

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Collection;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class WriteBatcher<K extends Comparable<K>, P> {

  private final Logger log;
  private final KeySpec<K> keySpec;
  private final int kafkaPartition;
  private final FlushManager<K, P> flushManager;

  private int numTablePartitionsInBatch;

  public WriteBatcher(
      final KeySpec<K> keySpec,
      final int kafkaPartition,
      final FlushManager<K, P> flushManager
  ) {
    this.keySpec = keySpec;
    this.kafkaPartition = kafkaPartition;
    this.flushManager = flushManager;
    this.log = new LogContext(flushManager.logPrefix()).logger(WriteBatcher.class);
  }

  public RemoteWriteResult<P> flushWriteBatch(
      final Collection<Result<K>> bufferedWrites,
      final long consumedOffset
  ) {
    log.info("SOPHIE: beginning new write batch");

    final WriteBatch<K, P> writeBatch = new WriteBatch<>(
        flushManager::createWriter,
        keySpec,
        kafkaPartition,
        flushManager.partitioner(),
        bufferedWrites
    );
    log.info("SOPHIE: created write batch, about to prepare flush");

    numTablePartitionsInBatch = writeBatch.numTablePartitionsInBatch();

    final var preFlushResult = flushManager.preFlush();
    if (!preFlushResult.wasApplied()) {
      log.warn("Failed on pre-flush callback for write batch (consumedOffset={})", consumedOffset);
      return preFlushResult;
    }

    log.info("SOPHIE: finished prep and ready to flush write batch");

    final var flushResult = writeBatch.flushBatch();
    // the offset is only used for recovery, so it can (and should) be set only
    // if/when the entire batch of flushes has completed successfully
    if (!flushResult.wasApplied()) {
      log.warn("Failed to flush write batch (consumedOffset={}) on table partition {}",
               consumedOffset, flushResult.tablePartition());
      return flushResult;
    } else {
      log.info("SOPHIE: finished flushing write batch and executing postFlush");

      final var postFlushResult = flushManager.postFlush(consumedOffset);
      if (!postFlushResult.wasApplied()) {
        log.warn("Failed on post-flush callback for write batch (consumedOffset={})",
                 consumedOffset);
      }
      return postFlushResult;
    }
  }

  public String failedFlushMsg(
      final RemoteWriteResult<P> result,
      final long consumedOffset
  ) {
    return String.format(
        "Error while writing batch for table partition %s! %s",
        result.tablePartition(), flushManager.failedFlushMsg(consumedOffset)
    );
  }

  public String successfulFlushMsg() {
    return String.format(
        "numPartitionsFlushed=%d, %s", numTablePartitionsInBatch, flushManager.successfulFlushMsg()
    );
  }
}
