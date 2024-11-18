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

package dev.responsive.kafka.internal.db.partitioning;

import dev.responsive.kafka.internal.utils.SessionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionSegmentPartitioner implements
    TablePartitioner<SessionKey, Segmenter.SegmentPartition> {

  private static final Logger LOG = LoggerFactory.getLogger(SessionSegmentPartitioner.class);
  private static final long METADATA_SEGMENT_ID = -1L;

  private final Segmenter segmenter;

  public SessionSegmentPartitioner(final long retentionPeriodMs, final long segmentIntervalMs) {
    this.segmenter = new Segmenter(retentionPeriodMs, segmentIntervalMs);
  }

  @Override
  public Segmenter.SegmentPartition tablePartition(
      final int kafkaPartition,
      final SessionKey key
  ) {
    return new Segmenter.SegmentPartition(
        kafkaPartition,
        this.segmenter.segmentStartTimestamp(key.sessionEndMs)
    );
  }

  @Override
  public Segmenter.SegmentPartition metadataTablePartition(final int kafkaPartition) {
    return new Segmenter.SegmentPartition(kafkaPartition, METADATA_SEGMENT_ID);
  }

  public Segmenter segmenter() {
    return this.segmenter;
  }
}
