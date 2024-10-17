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
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;

public class TtlProvider<K, V> {

  public static <K, V> TtlProvider<K, V> withDefault(final Duration defaultTtl) {
    return new TtlProvider<>(
        TtlType.DEFAULT_ONLY,
        TtlDuration.of(defaultTtl),
        (ignoredK, ignoredV) -> Optional.of(TtlDuration.of(defaultTtl)),
        null,
        null
    );
  }

  /**
   * @return a TtlProvider that will retain records indefinitely by default
   */
  public static <K, V> TtlProvider<K, V> withInfiniteDefault() {
    return new TtlProvider<>(
        TtlType.DEFAULT_ONLY,
        TtlDuration.noTtl(),
        (ignoredK, ignoredV) -> Optional.of(TtlDuration.noTtl()),
        null,
        null
    );
  }

  public TtlProvider<K, V> fromKey(
      final Function<K, Optional<TtlDuration>> computeTtlFromKey,
      final Serde<K> keySerde
  ) {
    if (ttlType.equals(TtlType.VALUE) || ttlType.equals(TtlType.KEY_AND_VALUE)) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<>(
        TtlType.KEY,
        defaultTtl,
        (k, ignored) -> computeTtlFromKey.apply(k),
        keySerde,
        null
    );
  }

  public TtlProvider<K, V> fromValue(
      final Function<V, Optional<TtlDuration>> computeTtlFromValue,
      final Serde<V> valueSerde
  ) {
    if (ttlType.equals(TtlType.KEY) || ttlType.equals(TtlType.KEY_AND_VALUE)) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<>(
        TtlType.VALUE,
        defaultTtl,
        (ignored, v) -> computeTtlFromValue.apply(v),
        null,
        valueSerde);
  }

  public TtlProvider<K, V> fromKeyAndValue(
      final BiFunction<K, V, Optional<TtlDuration>> computeTtlFromKeyAndValue,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    if (ttlType.equals(TtlType.KEY) || ttlType.equals(TtlType.VALUE)) {
      throw new IllegalArgumentException("Must choose only one of key, value, or key-and-value ttl");
    }
    return new TtlProvider<>(
        TtlType.KEY_AND_VALUE,
        defaultTtl,
        computeTtlFromKeyAndValue,
        keySerde,
        valueSerde
    );
  }

  public static class TtlDuration {

    public enum Ttl {
      INFINITE,
      FINITE
    }

    public static TtlDuration of(final Duration ttl) {
      if (ttl.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("ttl duration must be greater than zero");
      }
      return new TtlDuration(ttl, Ttl.FINITE);
    }

    // No ttl will be applied, in other words infinite retention
    public static TtlDuration noTtl() {
      return new TtlDuration(Duration.ZERO, Ttl.INFINITE);
    }

    private final Duration ttl;
    private final Ttl ttlType;

    private TtlDuration(final Duration ttlValue, final Ttl ttlType) {
      this.ttl = ttlValue;
      this.ttlType = ttlType;
    }

    public Duration ttl() {
      return ttl;
    }

    public boolean isFinite() {
      return ttlType.equals(Ttl.FINITE);
    }

    public int toSeconds() {
      return (int) ttl.toSeconds();
    }

    public long toMillis() {
      return ttl.toMillis();
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

  // Only non-null for key/value-based ttl providers
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;

  private final BiFunction<K, V, Optional<TtlDuration>> computeTtl;

  private TtlProvider(
      final TtlType ttlType,
      final TtlDuration defaultTtl,
      final BiFunction<K, V, Optional<TtlDuration>> computeTtl,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    this.ttlType = ttlType;
    this.defaultTtl = defaultTtl;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.computeTtl = computeTtl;
  }

  public Serde<K> keySerde() {
    return keySerde;
  }

  public Serde<V> valueSerde() {
    return valueSerde;
  }

  public TtlDuration defaultTtl() {
    return defaultTtl;
  }

  public boolean hasTtl() {
    return ttlType != TtlType.DEFAULT_ONLY || defaultTtl.isFinite();
  }

  public boolean hasConstantTtl() {
    return ttlType == TtlType.DEFAULT_ONLY;
  }

  public boolean canComputeWithoutValue() {
    return ttlType == TtlType.DEFAULT_ONLY || ttlType == TtlType.KEY;
  }

  public Optional<TtlDuration> computeTtl(
      final byte[] keyBytes,
      final byte[] valueBytes,
      final StateDeserializer<K, V> stateDeserializer
  ) {
    final K key;
    final V value;
    switch (ttlType) {
      case DEFAULT_ONLY:
        key = null; //ignored
        value = null; //ignored
        break;
      case KEY:
        key = stateDeserializer.keyFrom(keyBytes);
        value = null; //ignored
        break;
      case VALUE:
        key = null; //ignored
        value = stateDeserializer.valueFrom(valueBytes);
        break;
      case KEY_AND_VALUE:
        key = stateDeserializer.keyFrom(keyBytes);
        value = stateDeserializer.valueFrom(valueBytes);
        break;
      default:
        throw new IllegalStateException("Unrecognized ttl type: " + ttlType);
    }
    return computeTtl.apply(key, value);
  }

}