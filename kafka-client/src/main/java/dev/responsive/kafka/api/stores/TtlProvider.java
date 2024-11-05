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

public class TtlProvider<K, V> {

  /**
   * Creates a new TtlProvider with the given default duration to retain records for unless
   * overridden. To allow ttl overrides for individual records, you can use one of the
   * {@link #fromKey(Function)}, {@link #fromValue(Function)},
   * or {@link #fromKeyAndValue(BiFunction)} methods to define the row-level
   * override function.
   *
   * @return a new TtlProvider that will retain records for the specified default duration
   */
  public static <K, V> TtlProvider<K, V> withDefault(final Duration defaultTtl) {
    return new TtlProvider<>(
        TtlType.DEFAULT_ONLY,
        TtlDuration.of(defaultTtl),
        (ignoredK, ignoredV) -> Optional.empty()
    );
  }

  /**
   * Creates a new TtlProvider that has no default (equivalent to infinite retention for
   * all records unless an override is specified).  Must be used in combination with
   * exactly one of the {@link #fromKey(Function)}, {@link #fromValue(Function)},
   * and {@link #fromKeyAndValue(BiFunction)} methods to define the row-level
   * override function.
   *
   * @return a new TtlProvider that will retain records indefinitely by default
   */
  public static <K, V> TtlProvider<K, V> withNoDefault() {
    return new TtlProvider<>(
        TtlType.DEFAULT_ONLY,
        TtlDuration.infinite(),
        (ignoredK, ignoredV) -> Optional.empty()
    );
  }

  /**
   * @param computeTtlFromKey function that returns the ttl override for this specific key,
   *                          or {@link Optional#empty()} to use the default ttl
   *
   * @return the same TtlProvider with a key-based override function
   */
  public TtlProvider<K, V> fromKey(
      final Function<K, Optional<TtlDuration>> computeTtlFromKey
  ) {
    if (ttlType.equals(TtlType.VALUE) || ttlType.equals(TtlType.KEY_AND_VALUE)) {
      throw new IllegalArgumentException("Must choose only key, value, or key-and-value ttl");
    }

    return new TtlProvider<>(
        TtlType.KEY,
        defaultTtl,
        (k, ignored) -> computeTtlFromKey.apply(k)
    );
  }

  /**
   * @param computeTtlFromValue function that returns the ttl override for this specific value,
   *                            or {@link Optional#empty()} to use the default ttl
   * @return the same TtlProvider with a value-based override function
   */
  public TtlProvider<K, V> fromValue(
      final Function<V, Optional<TtlDuration>> computeTtlFromValue
  ) {
    if (ttlType.equals(TtlType.KEY) || ttlType.equals(TtlType.KEY_AND_VALUE)) {
      throw new IllegalArgumentException("Must choose only key, value, or key-and-value ttl");
    }

    return new TtlProvider<>(
        TtlType.VALUE,
        defaultTtl,
        (ignored, v) -> computeTtlFromValue.apply(v)
       );
  }

  /**
   * @param computeTtlFromKeyAndValue function that returns the ttl override for this specific key
   *                                  and value, or {@link Optional#empty()} to use the default ttl
   * @return the same TtlProvider with a key-and-value-based override function
   */
  public TtlProvider<K, V> fromKeyAndValue(
      final BiFunction<K, V, Optional<TtlDuration>> computeTtlFromKeyAndValue
  ) {
    if (ttlType.equals(TtlType.KEY) || ttlType.equals(TtlType.VALUE)) {
      throw new IllegalArgumentException("Must choose only key, value, or key-and-value ttl");
    }

    return new TtlProvider<>(
        TtlType.KEY_AND_VALUE,
        defaultTtl,
        computeTtlFromKeyAndValue
    );
  }

  public static class TtlDuration {

    public enum Ttl {
      INFINITE,
      FINITE
    }

    public static TtlDuration of(final Duration ttl) {
      if (ttl.compareTo(Duration.ZERO) <= 0) {
        throw new IllegalArgumentException("ttl duration must be greater than zero");
      }
      return new TtlDuration(ttl, Ttl.FINITE);
    }

    // No ttl will be applied, in other words infinite retention
    public static TtlDuration infinite() {
      return new TtlDuration(Duration.ZERO, Ttl.INFINITE);
    }

    private final Duration duration;
    private final Ttl ttlType;

    private TtlDuration(final Duration ttlValue, final Ttl ttlType) {
      this.duration = ttlValue;
      this.ttlType = ttlType;
    }

    public Duration duration() {
      if (!isFinite()) {
        throw new IllegalStateException("Can't convert TtlDuration to Duration unless finite");
      }
      return duration;
    }

    public boolean isFinite() {
      return ttlType.equals(Ttl.FINITE);
    }

    public long toSeconds() {
      return duration().toSeconds();
    }

    public long toMillis() {
      return duration().toMillis();
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

      if (!duration.equals(that.duration)) {
        return false;
      }
      return ttlType == that.ttlType;
    }

    @Override
    public int hashCode() {
      int result = duration.hashCode();
      result = 31 * result + ttlType.hashCode();
      return result;
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

  private final BiFunction<K, V, Optional<TtlDuration>> computeTtl;

  private TtlProvider(
      final TtlType ttlType,
      final TtlDuration defaultTtl,
      final BiFunction<K, V, Optional<TtlDuration>> computeTtl
  ) {
    this.ttlType = ttlType;
    this.defaultTtl = defaultTtl;
    this.computeTtl = computeTtl;
  }

  public TtlDuration defaultTtl() {
    return defaultTtl;
  }

  public boolean hasDefaultOnly() {
    return ttlType == TtlType.DEFAULT_ONLY;
  }

  public boolean needsValueToComputeTtl() {
    return ttlType == TtlType.VALUE || ttlType == TtlType.KEY_AND_VALUE;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TtlProvider<?, ?> that = (TtlProvider<?, ?>) o;

    if (ttlType != that.ttlType) {
      return false;
    }
    if (!defaultTtl.equals(that.defaultTtl)) {
      return false;
    }
    return computeTtl.equals(that.computeTtl);
  }

  @Override
  public int hashCode() {
    int result = ttlType.hashCode();
    result = 31 * result + defaultTtl.hashCode();
    result = 31 * result + computeTtl.hashCode();
    return result;
  }
}