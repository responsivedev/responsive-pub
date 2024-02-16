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

import dev.responsive.kafka.internal.stores.SchemaTypes.SessionSchema;
import dev.responsive.kafka.internal.utils.TableName;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ResponsiveSessionParams {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveSessionParams.class);

  // TODO: should we enforce a real upper bound on the number of segments/partitions that are
  //  created? Is there a maximum recommended for Scylla, or for this compaction strategy, etc?
  private static final long MINIMUM_ALLOWED_NUM_SEGMENTS = 2L;
  private static final long MAXIMUM_ALLOWED_NUM_SEGMENTS = Long.MAX_VALUE;
  private static final long MAXIMUM_DEFAULT_NUM_SEGMENTS = 100L;
  private static final long DEFAULT_NUM_SEGMENTS = 10L; // space amplification = 1.1x

  private final TableName name;
  private final SessionSchema schemaType;
  private final long inactivityGapMs;
  private final long gracePeriodMs;
  private final boolean retainDuplicates;
  private final long retentionPeriodMs;

  private long numSegments;
  private boolean truncateChangelog = false;

  private ResponsiveSessionParams(
      final String name,
      final SessionSchema schemaType,
      final Duration inactivityGap,
      final Duration gracePeriod,
      final boolean retainDuplicates
  ) {
    this.name = new TableName(name);
    this.schemaType = schemaType;
    this.inactivityGapMs = durationToMillis(inactivityGap, "inactivityGap");
    this.gracePeriodMs = durationToMillis(gracePeriod, "gracePeriod");
    this.retainDuplicates = retainDuplicates;

    this.retentionPeriodMs = this.inactivityGapMs + this.gracePeriodMs;
    this.numSegments = computeDefaultNumSegments(retentionPeriodMs);
  }

  public static ResponsiveSessionParams session(
      final String name,
      final Duration inactivityGap,
      final Duration gracePeriod
  ) {
    return new ResponsiveSessionParams(
        name, SessionSchema.SESSION, inactivityGap, gracePeriod, false
    );
  }

  public ResponsiveSessionParams withTruncateChangelog() {
    this.truncateChangelog = true;
    return this;
  }

  public SessionSchema schemaType() {
    return this.schemaType;
  }

  public TableName name() {
    return this.name;
  }

  public long retentionPeriod() {
    return this.retentionPeriodMs;
  }

  public boolean retainDuplicates() {
    return this.retainDuplicates;
  }

  public long numSegments() {
    return this.numSegments;
  }

  public boolean truncateChangelog() {
    return this.truncateChangelog;
  }

  private static long maximumAllowedNumSegments(final long retentionPeriodMs) {
    return Math.min(MAXIMUM_ALLOWED_NUM_SEGMENTS, retentionPeriodMs);
  }

  private static long computeDefaultNumSegments(final long retentionPeriodMs) {
    // TODO: Smart implementation.
    return DEFAULT_NUM_SEGMENTS;
  }
}
