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

package dev.responsive.kafka.api;

import static dev.responsive.utils.StoreUtil.durationToMillis;

import dev.responsive.kafka.store.SchemaType;
import dev.responsive.utils.TableName;
import java.time.Duration;

public final class ResponsiveWindowParams {

  private static final int DEFAULT_NUM_SEGMENTS = 4096;

  private final TableName name;
  private final SchemaType schemaType;
  private final long windowSizeMs;
  private final long gracePeriodMs;
  private final boolean retainDuplicates;

  private int numSegments;

  private ResponsiveWindowParams(
      final String name,
      final SchemaType schemaType,
      final Duration windowSize,
      final Duration gracePeriod,
      final int numSegments,
      final boolean retainDuplicates
  ) {
    this.name = new TableName(name);
    this.schemaType = schemaType;
    this.windowSizeMs = durationToMillis(windowSize, "windowSize");
    this.gracePeriodMs = durationToMillis(gracePeriod, "gracePeriod");
    this.numSegments = numSegments;
    this.retainDuplicates = retainDuplicates;
  }

  public static ResponsiveWindowParams window(
      final String name,
      final Duration windowSize,
      final Duration gracePeriod
  ) {
    return new ResponsiveWindowParams(
        name, SchemaType.WINDOW, windowSize, gracePeriod, DEFAULT_NUM_SEGMENTS, false
    );
  }

  public static ResponsiveWindowParams streamStreamJoin(
      final String name,
      final Duration windowSize,
      final Duration gracePeriod
  ) {
    final ResponsiveWindowParams ret = new ResponsiveWindowParams(
            name, SchemaType.STREAM, windowSize, gracePeriod, DEFAULT_NUM_SEGMENTS, true
    );

    throw new UnsupportedOperationException("Window stores for stream-stream joins have not yet "
                                                + "been implemented, please contact us if needed");
  }

  public ResponsiveWindowParams withNumSegments(final int numSegments) {
    this.numSegments = numSegments;
    return this;
  }

  public SchemaType schemaType() {
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

  public int numSegments() {
    return numSegments;
  }

  public boolean retainDuplicates() {
    return retainDuplicates;
  }

}
