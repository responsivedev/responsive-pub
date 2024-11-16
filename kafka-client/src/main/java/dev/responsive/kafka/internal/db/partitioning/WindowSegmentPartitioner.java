/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db.partitioning;

import dev.responsive.kafka.internal.utils.WindowedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowSegmentPartitioner implements
    TablePartitioner<WindowedKey, Segmenter.SegmentPartition> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowSegmentPartitioner.class);
  private static final long METADATA_SEGMENT_ID = -1L;

  private final Segmenter segmenter;
  private final boolean retainDuplicates;

  public WindowSegmentPartitioner(
      final long retentionPeriodMs,
      final long segmentIntervalMs,
      final boolean retainDuplicates
  ) {
    this.segmenter = new Segmenter(retentionPeriodMs, segmentIntervalMs);
    this.retainDuplicates = retainDuplicates;
  }

  @Override
  public Segmenter.SegmentPartition tablePartition(
      final int kafkaPartition,
      final WindowedKey key
  ) {
    return new Segmenter.SegmentPartition(
        kafkaPartition,
        this.segmenter.segmentStartTimestamp(key.windowStartMs)
    );
  }

  @Override
  public Segmenter.SegmentPartition metadataTablePartition(final int kafkaPartition) {
    return new Segmenter.SegmentPartition(kafkaPartition, METADATA_SEGMENT_ID);
  }

  public Segmenter segmenter() {
    return this.segmenter;
  }

  public boolean retainDuplicates() {
    return retainDuplicates;
  }
}
