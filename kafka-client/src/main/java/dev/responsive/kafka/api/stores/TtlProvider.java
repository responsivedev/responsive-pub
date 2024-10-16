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

import dev.responsive.kafka.internal.utils.StateDeserializer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TtlProvider<K, V> {

  public static TtlProvider<?, ?> withDefaultTtl(final Duration defaultTtl) {
    return new TtlProvider<>(TtlType.DEFAULT_ONLY, TtlDuration.ofTtl(defaultTtl), null, null, null);
  }

  /**
   * @return a TtlProvider that will retain records indefinitely by default
   */
  public static TtlProvider withInfiniteDefaultTtl() {
    return new TtlProvider(
        TtlType.DEFAULT_ONLY,
        TtlDuration.INFINITE_TTL,
        null,
        null,
        null
    );
  }

  public TtlProvider<K, ?> withKeyBasedTtl(final Function<K, Optional<TtlDuration>> ttlForKey) {
    if (ttlForValue != null || ttlForKeyAndValue != null) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<K, Object>(TtlType.KEY, defaultTtl, ttlForKey, null, null);
  }

  public TtlProvider<?, V> withValueBasedTtl(
      final Function<V, Optional<TtlDuration>> ttlForValue
  ) {
    if (ttlForKey != null || ttlForKeyAndValue != null) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<>(TtlType.VALUE, defaultTtl, null, ttlForValue, null);
  }

  public TtlProvider<K, V> withKeyAndValueBasedTtl(
      final BiFunction<K, V, Optional<TtlDuration>> ttlForKeyAndValue
  ) {
    if (ttlForKey != null || ttlForValue != null) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<>(TtlType.KEY_AND_VALUE, defaultTtl, null, null, ttlForKeyAndValue);
  }

  public static class TtlDuration {

    /* Equivalent to no ttl -- records are never expired */
    public static final TtlDuration INFINITE_TTL = new TtlDuration(Duration.ZERO);

    public static TtlDuration ofTtl(final Duration ttl) {
      if (ttl.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("ttl duration must be greater than zero");
      }
      return new TtlDuration(ttl);
    }

    private final Duration ttl;

    private TtlDuration(final Duration ttlValue) {
      this.ttl = ttlValue;
    }

    public Duration ttl() {
      return ttl;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final TtlDuration that = (TtlDuration) o;

      return ttl.equals(that.ttl);
    }

    @Override
    public int hashCode() {
      return ttl.hashCode();
    }
  }

  private enum TtlType {
    DEFAULT_ONLY,
    KEY,
    VALUE,
    KEY_AND_VALUE
  }

  private final TtlType ttlType;
  private final TtlDuration defaultTtl;

  // Only 1 of these is used per instance, others will be null
  private final Function<K, Optional<TtlDuration>> ttlForKey;
  private final Function<V, Optional<TtlDuration>> ttlForValue;
  private final BiFunction<K, V, Optional<TtlDuration>> ttlForKeyAndValue;

  public TtlProvider(
      final TtlType ttlType,
      final TtlDuration defaultTtl,
      final Function<K, Optional<TtlDuration>> ttlForKey,
      final Function<V, Optional<TtlDuration>> ttlForValue,
      final BiFunction<K, V, Optional<TtlDuration>> ttlForKeyAndValue
  ) {
    this.ttlType = ttlType;
    this.defaultTtl = defaultTtl;
    this.ttlForKey = ttlForKey;
    this.ttlForValue = ttlForValue;
    this.ttlForKeyAndValue = ttlForKeyAndValue;
  }

  public TtlDuration defaultTtl() {
    return defaultTtl;
  }

  public TtlDuration computeTtl(
      final byte[] keyBytes,
      final byte[] valueBytes,
      final StateDeserializer<K, V> stateDeserializer
  ) {
    switch (ttlType) {
      case DEFAULT_ONLY:
        return defaultTtl;
      case KEY:
        final Optional<TtlDuration> keyOverride = ttlForKey.apply(
            stateDeserializer.keyFrom(keyBytes)
        );
        return keyOverride.orElse(defaultTtl);
      case VALUE:
        final Optional<TtlDuration> valueOverride = ttlForValue.apply(
            stateDeserializer.valueFrom(valueBytes)
        );
        return valueOverride.orElse(defaultTtl);
      case KEY_AND_VALUE:
        final Optional<TtlDuration> keyAndValueOverride = ttlForKeyAndValue.apply(
            stateDeserializer.keyFrom(keyBytes),
            stateDeserializer.valueFrom(valueBytes)
        );
        return keyAndValueOverride.orElse(defaultTtl);
      default:
        throw new IllegalStateException("Unrecognized ttl type: " + ttlType);
    }
  }

}