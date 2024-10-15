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

package dev.responsive.kafka.api.stores;

import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;

public interface TtlProvider<K, V> {

  enum TtlOptions {
    DEFAULT_ONLY,
    KEY,
    VALUE,
    KEY_AND_VALUE
  }

  TtlOptions options();

  Optional<Duration> defaultTtl();

  Optional<Duration> computeTtl(final K key, final V value);

  class Resolver<K, V> {

    private final Deserializer<K> keySerde;
    private final Deserializer<V> valueSerde;
    private final TtlProvider<K, V> provider;
    private final String topic;

    public Resolver(
        final String topic,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final TtlProvider<K, V> provider
    ) {
      this.topic = topic;
      this.keySerde = keySerde.deserializer();
      this.valueSerde = valueSerde.deserializer();
      this.provider = provider;
    }

    public Optional<Duration> compute(final Bytes keyBytes, final byte[] valueBytes) {
      K key = null;
      V value = null;

      switch (provider.options()) {
        case DEFAULT_ONLY:
          return provider.defaultTtl();
        case KEY:
          key = keySerde.deserialize(topic, keyBytes.get());
          break;
        case VALUE:
          value = valueSerde.deserialize(topic, valueBytes);
          break;
        case KEY_AND_VALUE:
          key = keySerde.deserialize(topic, keyBytes.get());
          value = valueSerde.deserialize(topic, valueBytes);
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + provider.options());
      }

      final var ttl = provider.computeTtl(key, value);
      return ttl.isPresent() ? ttl : provider.defaultTtl();
    }

  }

}