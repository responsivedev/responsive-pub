/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.PendingFlushSegmentMetadata;
import dev.responsive.kafka.internal.utils.SessionKey;

public abstract class SessionFlushManager implements FlushManager<SessionKey, SegmentPartition> {

  private final int kafkaPartition;
  private final Segmenter segmenter;
  private final PendingFlushSegmentMetadata pendingFlushSegmentMetadata;

  public SessionFlushManager(
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
  public void writeAdded(final SessionKey key) {
    pendingFlushSegmentMetadata.updateStreamTime(key.sessionEndMs);
  }

  @Override
  public RemoteWriteResult<SegmentPartition> preFlush() {
    final var pendingRoll = pendingFlushSegmentMetadata.prepareRoll(segmenter);

    for (final long segmentStartTimestamp : pendingRoll.segmentsToCreate()) {
      final var createResult =
          createSegment(new SegmentPartition(kafkaPartition, segmentStartTimestamp));
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

    for (final long segmentStartTimestamp : pendingFlushSegmentMetadata.segmentRoll()
        .segmentsToExpire()) {
      final var deleteResult =
          deleteSegment(new SegmentPartition(kafkaPartition, segmentStartTimestamp));
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
