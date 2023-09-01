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

import dev.responsive.kafka.store.ResponsiveStore;
import dev.responsive.kafka.store.ResponsiveTimestampedStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

  private final ResponsiveKeyValueParams params;

  public ResponsiveKeyValueBytesStoreSupplier(final ResponsiveKeyValueParams params) {
    this.params = params;
  }

  @Override
  public String name() {
    return params.name().kafkaName();
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    if (params.isTimestamped()) {
      return new ResponsiveTimestampedStore(params);
    } else {
      return new ResponsiveStore(params);
    }
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}