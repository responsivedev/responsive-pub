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

import dev.responsive.kafka.store.SchemaTypes.KVSchema;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

public final class ResponsiveKeyValueParams {

  private final TableName name;
  private final KVSchema schema;
  private final boolean timestamped;

  @Nullable private Duration timeToLive = null;
  private boolean truncateChangelog = false;

  private ResponsiveKeyValueParams(
      final String name,
      final boolean timestamped,
      final KVSchema schema
  ) {
    this.name = new TableName(name);
    this.timestamped = timestamped;
    this.schema = schema;
  }

  public static ResponsiveKeyValueParams keyValue(final String name) {
    return new ResponsiveKeyValueParams(name, false, KVSchema.KEY_VALUE);
  }

  public static ResponsiveKeyValueParams timestamped(final String name) {
    return new ResponsiveKeyValueParams(name, true, KVSchema.KEY_VALUE);
  }

  public static ResponsiveKeyValueParams fact(final String name) {
    return new ResponsiveKeyValueParams(name, false, KVSchema.FACT);
  }

  public static ResponsiveKeyValueParams timestampedFact(final String name) {
    return new ResponsiveKeyValueParams(name, true, KVSchema.FACT);
  }

  public ResponsiveKeyValueParams withTimeToLive(final Duration timeToLive) {
    this.timeToLive = timeToLive;
    return this;
  }

  public ResponsiveKeyValueParams withTruncateChangelog() {
    this.truncateChangelog = true;
    return this;
  }

  public TableName name() {
    return name;
  }

  public boolean isTimestamped() {
    return timestamped;
  }

  public KVSchema schemaType() {
    return schema;
  }

  public Optional<Duration> timeToLive() {
    return Optional.ofNullable(timeToLive);
  }

  public boolean truncateChangelog() {
    return truncateChangelog;
  }
}
