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
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;

public class TtlResolver<K, V> {

  public static final Optional<TtlResolver<?, ?>> NO_TTL = Optional.empty();

  private final StateDeserializer<K, V> stateDeserializer;
  private final TtlProvider<K, V> ttlProvider;

  public static Optional<TtlResolver<?, ?>> fromTtlProvider(
      final boolean isTimestamped,
      final String changelogTopic,
      final Optional<TtlProvider<?, ?>> ttlProvider
  ) {
    return ttlProvider.isPresent()
        ? Optional.of(new TtlResolver<>(isTimestamped, changelogTopic, ttlProvider.get()))
        : Optional.empty();
  }

  public TtlResolver(
      final boolean isTimestamped,
      final String changelogTopic,
      final TtlProvider<K, V> ttlProvider
  ) {
    this.stateDeserializer = new StateDeserializer<>(
        isTimestamped,
        changelogTopic,
        ttlProvider.keySerde(),
        ttlProvider.valueSerde()
    );
    this.ttlProvider = ttlProvider;
  }

  public TtlDuration defaultTtl() {
    return ttlProvider.defaultTtl();
  }

  public boolean hasConstantTtl() {
    return ttlProvider.hasDefaultOnly();
  }

  public boolean needsValueToComputeTtl() {
    return ttlProvider.needsValueToComputeTtl();
  }

  public Optional<TtlDuration> computeTtl(final Bytes keyBytes, final byte[] valueBytes) {
    return ttlProvider.computeTtl(keyBytes.get(), valueBytes, stateDeserializer);
  }

  public TtlDuration resolveTtl(final Bytes keyBytes, final byte[] valueBytes) {
    final Optional<TtlDuration> ttl = computeTtl(keyBytes, valueBytes);
    return ttl.orElse(defaultTtl());
  }

}
