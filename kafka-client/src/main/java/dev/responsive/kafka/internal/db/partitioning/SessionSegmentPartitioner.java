/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.db.partitioning;

import dev.responsive.kafka.internal.utils.SessionKey;
import org.apache.kafka.common.utils.Bytes;
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

  @Override
  public boolean belongs(final Bytes key, final int kafkaPartition) {
    throw new UnsupportedOperationException(
        "SessionSegmentPartitioner relies on specific mapping of kafka partition to key. This "
            + "method should not be called as the assumption is that only keys "
            + "stored for the given partition will be returned. If this exception "
            + "is seen in production please file a ticket with the Responsive team.");
  }

  public Segmenter segmenter() {
    return this.segmenter;
  }
}
