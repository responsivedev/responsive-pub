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

import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveTimestampedKeyValueStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

  private final ResponsiveKeyValueParams params;

  private boolean isTimestamped;

  public ResponsiveKeyValueBytesStoreSupplier(final ResponsiveKeyValueParams params) {
    this.params = params;
  }

  public void asTimestamped() {
    this.isTimestamped = true;
  }

  @Override
  public String name() {
    return params.name().kafkaName();
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    if (isTimestamped) {
      return new ResponsiveTimestampedKeyValueStore(params);
    } else {
      return new ResponsiveKeyValueStore(params, isTimestamped);
    }
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}