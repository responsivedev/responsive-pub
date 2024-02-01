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

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentRoll;

/**
 * Used to track metadata of the segments that make up a windowed table, such as
 * the latest flushed stream-time and the stream-time of the current batch.
 * Note: we intentionally track stream-time separately here and in the state store itself
 * as these entities have different views of the current time and should not be unified.
 * (Specifically, the table will always lag the view of stream-time that is shared by the
 * ResponsiveWindowStore and CommitBuffer due to buffering/batching of writes)
 */
public class SegmentBatch {

  public long flushedStreamTime;
  public long batchStreamTime;
  public SegmentRoll segmentRoll;

  public SegmentBatch(final long persistedStreamTime) {
    this.flushedStreamTime = persistedStreamTime;
    this.batchStreamTime = persistedStreamTime;
  }

  public void updateStreamTime(final long recordTimestamp) {
    batchStreamTime = Math.max(batchStreamTime, recordTimestamp);
  }

  public void prepareRoll(final SegmentRoll newSegmentRoll) {
    segmentRoll = newSegmentRoll;
  }

  public void finalizeRoll() {
    segmentRoll = null;
    flushedStreamTime = batchStreamTime;
  }

}