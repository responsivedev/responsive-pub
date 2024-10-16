/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.internal.utils.StateDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;

public class TtlResolver<K, V> {

  private final StateDeserializer<K, V> stateDeserializer;
  private final TtlProvider<K, V> ttlProvider;

  public TtlResolver(
      final String changelogTopic,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final TtlProvider<K, V> ttlProvider
  ) {
    this.stateDeserializer = new StateDeserializer<>(
        changelogTopic,
        keySerde.deserializer(),
        valueSerde.deserializer()
    );
    this.ttlProvider = ttlProvider;
  }

  public TtlDuration resolveTtl(final Bytes keyBytes, final byte[] valueBytes) {
    return ttlProvider.computeTtl(keyBytes.get(), valueBytes, stateDeserializer);
  }

}
