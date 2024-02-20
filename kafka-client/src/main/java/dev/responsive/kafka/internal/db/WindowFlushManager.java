/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.PendingFlushSegmentMetadata;
import dev.responsive.kafka.internal.utils.WindowedKey;

public abstract class WindowFlushManager implements FlushManager<WindowedKey, SegmentPartition> {

  private final int kafkaPartition;
  private final Segmenter segmenter;
  private final PendingFlushSegmentMetadata pendingFlushSegmentMetadata;

  public WindowFlushManager(
      final String tableName,
      final int kafkaPartition,
      final Segmenter segmenter,
      final long streamTime
  ) {
    this.kafkaPartition = kafkaPartition;
    this.segmenter = segmenter;
    this.pendingFlushSegmentMetadata =
        new PendingFlushSegmentMetadata(tableName, kafkaPartition, streamTime);
  }

  public long streamTime() {
    return pendingFlushSegmentMetadata.batchStreamTime();
  }

  @Override
  public void writeAdded(final WindowedKey key) {
    pendingFlushSegmentMetadata.updateStreamTime(key.windowStartMs);
  }

  @Override
  public RemoteWriteResult<SegmentPartition> preFlush() {
    final var pendingRoll = pendingFlushSegmentMetadata.prepareRoll(segmenter);

    for (final long segmentId : pendingRoll.segmentsToCreate()) {
      final var createResult = createSegment(new SegmentPartition(kafkaPartition, segmentId));
      if (!createResult.wasApplied()) {
        return createResult;
      }
    }

    return RemoteWriteResult.success(null);
  }

  @Override
  public RemoteWriteResult<SegmentPartition> postFlush(final long consumedOffset) {

    final var metadataResult = updateOffsetAndStreamTime(
        consumedOffset,
        pendingFlushSegmentMetadata.batchStreamTime()
    );
    if (!metadataResult.wasApplied()) {
      return metadataResult;
    }

    for (final long segmentId : pendingFlushSegmentMetadata.segmentRoll().segmentsToExpire()) {
      final var deleteResult = deleteSegment(new SegmentPartition(kafkaPartition, segmentId));
      if (!deleteResult.wasApplied()) {
        return deleteResult;
      }
    }

    pendingFlushSegmentMetadata.finalizeRoll();
    return RemoteWriteResult.success(null);
  }

  /**
   * Persist the latest consumed offset and stream-time corresponding to the batch that was just
   * flushed to the remote table
   */
  protected abstract RemoteWriteResult<SegmentPartition> updateOffsetAndStreamTime(
      final long consumedOffset,
      final long streamTime
  );

  /**
   * "Create" the passed-in segment by executing whatever preparations are needed to
   * support writes to this segment. Assumed to be completed synchronously
   */
  protected abstract RemoteWriteResult<SegmentPartition> createSegment(
      final SegmentPartition partition
  );

  /**
   * "Delete" the passed-in expired segment by executing whatever cleanup is needed to
   * release the resources held by this segment and reclaim the storage it previously held
   */
  protected abstract RemoteWriteResult<SegmentPartition> deleteSegment(
      final SegmentPartition partition
  );

}
