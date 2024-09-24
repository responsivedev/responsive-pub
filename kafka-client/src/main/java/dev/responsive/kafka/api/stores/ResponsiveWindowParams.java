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

package dev.responsive.kafka.api.stores;

import static dev.responsive.kafka.internal.utils.StoreUtil.durationToMillis;

import dev.responsive.kafka.internal.db.CassandraKeyValueTable;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import dev.responsive.kafka.internal.utils.TableName;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ResponsiveWindowParams {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveWindowParams.class);

  // TODO: should we enforce a real upper bound on the number of segments/partitions that are
  //  created? Is there a maximum recommended for Scylla, or for this compaction strategy, etc?
  private static final long MINIMUM_ALLOWED_NUM_SEGMENTS = 2L;
  private static final long MAXIMUM_ALLOWED_NUM_SEGMENTS = Long.MAX_VALUE;
  private static final long MAXIMUM_DEFAULT_NUM_SEGMENTS = 100L;
  private static final long DEFAULT_NUM_SEGMENTS = 10L; // space amplification = 1.1x

  private final TableName name;
  private final WindowSchema schemaType;
  private final long windowSizeMs;
  private final boolean retainDuplicates;
  private final long retentionPeriodMs;

  private long numSegments;

  private ResponsiveWindowParams(
      final String name,
      final WindowSchema schemaType,
      final Duration windowSize,
      final Duration retentionPeriod,
      final boolean retainDuplicates
  ) {
    this.name = new TableName(name);
    this.schemaType = schemaType;
    this.windowSizeMs = durationToMillis(windowSize, "windowSize");
    this.retentionPeriodMs = durationToMillis(retentionPeriod, "retentionPeriod");
    this.retainDuplicates = retainDuplicates;

    this.numSegments = computeDefaultNumSegments(windowSizeMs, retentionPeriodMs);
  }

  public static ResponsiveWindowParams window(
      final String name,
      final Duration windowSize,
      final Duration retentionPeriod,
      final boolean retainDuplicates
  ) {
    return new ResponsiveWindowParams(
        name, WindowSchema.WINDOW, windowSize, retentionPeriod, retainDuplicates
    );
  }

  public static ResponsiveWindowParams streamStreamJoin(
      final String name,
      final Duration windowSize
  ) {
    return new ResponsiveWindowParams(
            name, WindowSchema.WINDOW, windowSize, windowSize, true
    );
  }

  public ResponsiveWindowParams withNumSegments(final long numSegments) {
    final long maximumSegmentCount = maximumAllowedNumSegments(retentionPeriodMs);
    if (numSegments > maximumSegmentCount) {
      LOG.warn("Attempted to divide window store {} into more segments ({}) than is possible. "
                   + "Will use the maximum possible number of segments for this store, as"
                   + "determined by the retention period ({})",
               name, numSegments, maximumSegmentCount);
      this.numSegments = maximumSegmentCount;
    } else if (numSegments < MINIMUM_ALLOWED_NUM_SEGMENTS) {
      LOG.warn("Attempted to divide window store {} into fewer segments ({}) than is allowed. "
                   + "Will use the minimum allowable number of segments for this store ({})",
               name, numSegments, MINIMUM_ALLOWED_NUM_SEGMENTS);
    } else {
      this.numSegments = numSegments;
    }
    return this;
  }

  public WindowSchema schemaType() {
    return schemaType;
  }

  public TableName name() {
    return name;
  }

  public long windowSize() {
    return windowSizeMs;
  }

  public long retentionPeriod() {
    return retentionPeriodMs;
  }

  public boolean retainDuplicates() {
    return retainDuplicates;
  }

  public long numSegments() {
    return numSegments;
  }

  private static long maximumAllowedNumSegments(final long retentionPeriodMs) {
    return Math.min(MAXIMUM_ALLOWED_NUM_SEGMENTS, retentionPeriodMs);
  }

  /**
   * The general approach here is specific to C*, in which each segment is a separate table
   * partition similar to a subpartition in the {@link CassandraKeyValueTable}.
   * <p>
   * We probably don't need a huge number of table partitions for parallelism reasons, but
   * should have sufficient segments to make sure we are able to delete expired records
   * at a good enough pace to keep the additional storage cost low.
   * <p>
   * The absolute minimum number of segments should be 2, which gives a space amplification
   * of 1.5x the total size of the working data set (fairly large as-is, we may want a larger
   * minimum). We probably also want to cap off the maximum at some point but for now, we just
   * enforce that the num_segments is no larger than the retention period.
   * <p>
   * Given these limits, we select a general default for most cases based on a target
   * storage amplification factor. In addition, we consider the special case in which
   * the grace period is much larger than the window size. For this case, to optimize for
   * good caching we want to try and have the entire active window fit into the latest segment.
   */
  // TODO: experiment to determine a good default, and consider how to adapt this computation
  //  for a different storage engine or table implementation
  private static long computeDefaultNumSegments(
      final long windowSizeMs,
      final long retentionPeriodMs
  ) {
    // If we're able to fit the full window size in a single segment by using more than the
    // DEFAULT_NUM_SEGMENTS, do so by setting the segment interval equal to the window size
    // (where segmentInterval = retentionPeriod / numSegments
    if (windowSizeMs < retentionPeriodMs / DEFAULT_NUM_SEGMENTS) {
      return Math.min(MAXIMUM_DEFAULT_NUM_SEGMENTS, retentionPeriodMs / windowSizeMs);
    } else {
      return DEFAULT_NUM_SEGMENTS;
    }
  }

}
