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
import org.apache.kafka.common.utils.Bytes;

public class WriteManager<K extends Comparable<K>, P> {

  private final KeySpec<K> keySpec;
  private final int kafkaPartition;
  private final TablePartitioner<K, P> partitioner;
  private final FlushListener<K, P> flushListener;

  public RemoteWriteResult<P> flushWriteBatch(
      final Collection<Result<K>> bufferedWrites,
      final long consumedOffset
  ) {
    final WriteBatch<K, P> writeBatch = new WriteBatch<>(
        flushListener::createWriter, keySpec, kafkaPartition, partitioner, bufferedWrites
    );

    final var preFlushResult = flushListener.preFlush();
    // TODO(sophie): check result

    final var flushResult = writeBatch.flushBatch();
    // the offset is only used for recovery, so it can (and should) be set only
    // if/when the entire batch of flushes has completed successfully
    if (!flushResult.wasApplied()) {
      log.info("Failed to flush writes to table partition {}", flushResult.tablePartition());
      // TODO(sophie) -- call failedFlushError() or otherwise log useful info like offset,  epoch, etc
      return flushResult;
    } else {
      log.debug("Successfully flushed writes to table partition {}", flushResult.tablePartition());
      // TODO(sophie): consolidate error handling by checking result of postFlush here or throwing standard
      //  error after checking results and dealing with it in caller
      return flushListener.postFlush(consumedOffset);
    }
  }

  public String failedFlushError(
      final RemoteWriteResult<Integer> result,
      final long consumedOffset
  ) {
    // this most likely is a fencing error, so make sure to add on all the information
    // that is relevant to fencing in the error message
    // TODO (sophie): use table to get remote stored epoch?
    final long persistedEpoch = tableMetadata.fetchEpoch(result.tablePartition());
    final long persistedOffset = offset();

    return String.format(
        "Error while writing batch for table partition %s! "
            + "<Batch Offset: %d, Persisted Offset: %d> and <Local Epoch: %s, Persisted Epoch: %d>",
        result.tablePartition(), consumedOffset, persistedOffset, epoch, persistedEpoch
    );
  }
}
