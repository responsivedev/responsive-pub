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

import dev.responsive.kafka.internal.stores.ResponsiveWindowStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class ResponsiveWindowedStoreSupplier implements WindowBytesStoreSupplier {

  private final ResponsiveWindowParams params;
  private final long segmentIntervalMs;

  public ResponsiveWindowedStoreSupplier(final ResponsiveWindowParams params) {
    this.params = params;
    this.segmentIntervalMs = params.retentionPeriod() / params.numSegments();
  }

  @Override
  public String name() {
    return params.name().kafkaName();
  }

  @Override
  public WindowStore<Bytes, byte[]> get() {
    return new ResponsiveWindowStore(params);
  }

  @Override
  public long segmentIntervalMs() {
    return segmentIntervalMs;
  }

  @Override
  public long windowSize() {
    return params.windowSize();
  }

  @Override
  public boolean retainDuplicates() {
    return params.retainDuplicates();
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