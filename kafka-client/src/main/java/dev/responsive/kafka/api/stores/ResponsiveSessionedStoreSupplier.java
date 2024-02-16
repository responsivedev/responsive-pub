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

import static dev.responsive.kafka.internal.utils.StoreUtil.computeSegmentInterval;

import dev.responsive.kafka.internal.stores.ResponsiveSessionStore;
import dev.responsive.kafka.internal.stores.ResponsiveWindowStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class ResponsiveSessionedStoreSupplier implements SessionBytesStoreSupplier {

  private final ResponsiveSessionParams params;
  private final long segmentIntervalMs;

  public ResponsiveSessionedStoreSupplier(final ResponsiveSessionParams params) {
    this.params = params;
    this.segmentIntervalMs = computeSegmentInterval(params.retentionPeriod(), params.numSegments());
  }

  @Override
  public String name() {
    return this.params.name().kafkaName();
  }

  @Override
  public SessionStore<Bytes, byte[]> get() {
    return new ResponsiveSessionStore(this.params);
  }

  @Override
  public long segmentIntervalMs() {
    return segmentIntervalMs;
  }

  @Override
  public long retentionPeriod() {
    return params.retentionPeriod();
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}