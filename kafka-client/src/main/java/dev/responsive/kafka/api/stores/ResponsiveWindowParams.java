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

import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import dev.responsive.kafka.internal.utils.TableName;
import java.time.Duration;

public final class ResponsiveWindowParams {

  private static final int DEFAULT_NUM_SEGMENTS = 4096;

  private final TableName name;
  private final WindowSchema schemaType;
  private final long windowSizeMs;
  private final long gracePeriodMs;
  private final boolean retainDuplicates;

  private int numSegments = DEFAULT_NUM_SEGMENTS;
  private boolean truncateChangelog = false;


  private ResponsiveWindowParams(
      final String name,
      final WindowSchema schemaType,
      final Duration windowSize,
      final Duration gracePeriod,
      final boolean retainDuplicates
  ) {
    this.name = new TableName(name);
    this.schemaType = schemaType;
    this.windowSizeMs = durationToMillis(windowSize, "windowSize");
    this.gracePeriodMs = durationToMillis(gracePeriod, "gracePeriod");
    this.retainDuplicates = retainDuplicates;
  }

  public static ResponsiveWindowParams window(
      final String name,
      final Duration windowSize,
      final Duration gracePeriod
  ) {
    return new ResponsiveWindowParams(
        name, WindowSchema.WINDOW, windowSize, gracePeriod, false
    );
  }

  public static ResponsiveWindowParams streamStreamJoin(
      final String name,
      final Duration windowSize,
      final Duration gracePeriod
  ) {
    final ResponsiveWindowParams ret = new ResponsiveWindowParams(
            name, WindowSchema.STREAM, windowSize, gracePeriod, true
    );

    throw new UnsupportedOperationException("Window stores for stream-stream joins have not yet "
                                                + "been implemented, please contact us if needed");
  }

  public ResponsiveWindowParams withNumSegments(final int numSegments) {
    this.numSegments = numSegments;
    return this;
  }

  public ResponsiveWindowParams withTruncateChangelog() {
    this.truncateChangelog = true;
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
    return windowSizeMs + gracePeriodMs;
  }

  public boolean retainDuplicates() {
    return retainDuplicates;
  }

  public int numSegments() {
    return numSegments;
  }

  public boolean truncateChangelog() {
    return truncateChangelog;
  }
}
