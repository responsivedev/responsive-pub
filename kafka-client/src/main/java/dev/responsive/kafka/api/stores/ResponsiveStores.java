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

import dev.responsive.kafka.internal.stores.ResponsiveMaterialized;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * A factory for creating Kafka Streams state stores on top of a Responsive storage backend.
 * Use these when building your {@link org.apache.kafka.streams.Topology} to easily swap in
 * Responsive stores wherever state is used.
 * <p>
 * See {@link org.apache.kafka.streams.state.Stores} for instructions on how to plug in custom
 * state stores and configure them.
 */
public final class ResponsiveStores {

  //////////////////////////// KeyValue Stores ////////////////////////////

  /**
   * See for example {@link Stores#inMemoryKeyValueStore(String)}. This method should be
   * preferred over {@link #keyValueStore(String)} as it provides additional options such
   * as TTL support and will always compile when new features are added to
   * {@code ResponsiveKeyValueParams}.
   *
   * @param params parameters for creation of the key value store
   * @return a supplier for a key-value store with the given options
   * that uses Responsive's storage for its backend
   */
  public static KeyValueBytesStoreSupplier keyValueStore(final ResponsiveKeyValueParams params) {
    return new ResponsiveKeyValueBytesStoreSupplier(params);
  }

  /**
   * See for example {@link Stores#inMemoryKeyValueStore(String)}
   *
   * @param name the store name
   * @return a supplier for a key-value store with the given options
   * that uses Responsive's storage for its backend
   */
  public static KeyValueBytesStoreSupplier keyValueStore(final String name) {
    return keyValueStore(ResponsiveKeyValueParams.keyValue(name));
  }

  /**
   * See for example {@link Stores#persistentKeyValueStore(String)}. A fact store
   * assumes that all writes for a given key will always have the same value. The
   * implementation does not enforce this constraint, instead it uses the assumption
   * to optimize the consistency protocol by allowing split-brain writes to go
   * unfenced.
   *
   * <p>Examples of usage patterns that make good use of a fact store:
   * <ul>
   *   <li>A deduplication store that records whether or not a key has been seen.</li>
   *   <li>Sensor data that reports measurements from sensors as time-series data.</li>
   * </ul>
   * </p>
   *
   * <p>Delete operations on fact tables, although supported, should be considered
   * optimizations; your application should not depend on the data in a fact table
   * being deleted during a split-brain situation. </p>
   *
   * @param name the store name
   * @return a supplier for a key-value store with the given options
   * that uses Responsive's storage for its backend
   */
  public static KeyValueBytesStoreSupplier factStore(final String name) {
    return keyValueStore(ResponsiveKeyValueParams.fact(name));
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link KeyValueStore} and connect it via the Processor API. If configuring
   * stateful DSL operators, use {@link #materialized(ResponsiveKeyValueParams)} instead.
   * <p>
   * See {@link Stores#keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
   * for more details.
   * <p>
   * Note: all of Responsive's key-value {@link KeyValueBytesStoreSupplier StoreSuppliers}
   * are compatible with this builder.
   * We recommend using {@link #keyValueStore(ResponsiveKeyValueParams)}.
   *
   * @param storeSupplier the key-value store supplier
   * @param keySerde      the key serde. If null, the default.key.serde config will be used
   * @param valueSerde    the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   * that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        Stores.keyValueStoreBuilder(storeSupplier, keySerde, valueSerde),
        false
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedKeyValueStore} and connect it via the Processor API. If configuring
   * stateful DSL operators, use {@link #materialized(ResponsiveKeyValueParams)} instead.
   * <p>
   * See {@link Stores#timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
   * for more details.
   * <p>
   * Note: all of Responsive's key-value {@link KeyValueBytesStoreSupplier StoreSuppliers}
   * are compatible with this builder (specifically, they are all timestamp-enabled).
   * We recommend using {@link #keyValueStore(ResponsiveKeyValueParams)}.
   *
   * @param storeSupplier the key-value store supplier
   * @param keySerde      the key serde. If null, the default.key.serde config will be used
   * @param valueSerde    the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   * that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        Stores.timestampedKeyValueStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde
        ),
        false
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link KeyValueStore}
   * and materialized in the DSL. If using the low-level Processor API, use
   * {@link #keyValueStoreBuilder} instead.
   *
   * @param params the store parameters
   * @return a Materialized configuration that can be used to build a key value store with the
   * given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final ResponsiveKeyValueParams params
  ) {
    return new ResponsiveMaterialized<>(
        Materialized.as(keyValueStore(params)),
        params.truncateChangelog()
    );
  }

  //////////////////////////// Window Stores ////////////////////////////

  /**
   * See for example {@link Stores#inMemoryWindowStore(String, Duration, Duration, boolean)}
   *
   * @param params the {@link ResponsiveWindowParams} for this store
   *               use {@link ResponsiveWindowParams#window(String, Duration, Duration)} for windowed
   *               aggregations in the DSL or PAPI stores with update semantics
   *               use {@link ResponsiveWindowParams#streamStreamJoin(String, Duration, Duration)}
   *               for stream-stream joins in the DSL  or PAPI stores with duplicates semantics
   * @return a supplier for a window store with the given options
   * that uses Responsive's storage for its backend
   */
  public static WindowBytesStoreSupplier windowStoreSupplier(final ResponsiveWindowParams params) {
    return new ResponsiveWindowedStoreSupplier(params);
  }

  /**
   * See for example {@link Stores#inMemoryWindowStore(String, Duration, Duration, boolean)}
   *
   * @param name             the store name
   * @param retentionPeriod  the retention period, must be greater than or equal to window size
   * @param windowSize       the window size, must be greater than 0
   * @param retainDuplicates whether to retain duplicates vs overwrite records, this must be false
   *                         for all DSL operators except stream-stream joins which require true
   * @return a supplier for a window store with the given options
   * that uses Responsive's storage for its backend
   */
  public static WindowBytesStoreSupplier windowStoreSupplier(
      final String name,
      final Duration retentionPeriod,
      final Duration windowSize,
      final boolean retainDuplicates
  ) {
    final Duration gracePeriod = retentionPeriod.minus(windowSize);
    if (windowSize.isNegative() || windowSize.isZero()) {
      throw new IllegalArgumentException("Window size cannot be negative or zero");
    } else if (gracePeriod.isNegative()) {
      throw new IllegalArgumentException("Retention period cannot be less than window size");
    }

    if (!retainDuplicates) {
      return new ResponsiveWindowedStoreSupplier(
          ResponsiveWindowParams.window(name, windowSize, gracePeriod)
      );
    } else {
      final ResponsiveWindowedStoreSupplier ret = new ResponsiveWindowedStoreSupplier(
          ResponsiveWindowParams.streamStreamJoin(name, windowSize, gracePeriod)
      );
      throw new UnsupportedOperationException("Stream-stream join stores not yet implemented");
    }
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive {@link WindowStore}
   * and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized} instead.
   * <p>
   * See also {@link Stores#windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}
   *
   * @param storeSupplier a window store supplier
   * @param keySerde      the key serde. If null, the default.key.serde config will be used
   * @param valueSerde    the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store with the given options
   * that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(
      final WindowBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        Stores.windowStoreBuilder(storeSupplier, keySerde, valueSerde),
        false
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedWindowStore} and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized} instead.
   * <p>
   * See also {@link Stores#timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}
   *
   * @param storeSupplier a timestamped window store supplier
   * @param keySerde      the key serde. If null, the default.key.serde config will be used
   * @param valueSerde    the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store with the given options
   * that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(
      final WindowBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        Stores.timestampedWindowStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde
        ),
        false
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link WindowStore}
   * and materialized in the DSL for most operators. If using the low-level Processor API,
   * use {@link #windowStoreBuilder}
   *
   * @param params the store parameters
   * @return a Materialized configuration that can be used to build a key value store with the
   * given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> windowMaterialized(
      final ResponsiveWindowParams params
  ) {
    return new ResponsiveMaterialized<>(
        Materialized.as(new ResponsiveWindowedStoreSupplier(params)),
        params.truncateChangelog()
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link SessionStore} and connect it via the Processor API. If using the DSL, use
   * {@link #sessionMaterialized} instead.
   * <p>
   *
   * @param storeSupplier a session store supplier
   * @param keySerde      the key serde. If null, the default.key.serde config will be used
   * @param valueSerde    the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a session store with the given options
   * that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(
      final SessionBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        Stores.sessionStoreBuilder(storeSupplier, keySerde, valueSerde),
        false
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link SessionStore}
   * and materialized in the DSL for most operators. If using the low-level Processor API,
   * use {@link #sessionStoreBuilder}
   *
   * @param params the store parameters
   * @return a Materialized configuration that can be used to build a key value store with the
   * given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, SessionStore<Bytes, byte[]>> sessionMaterialized(
      final ResponsiveSessionParams params
  ) {
    return new ResponsiveMaterialized<K, V, SessionStore<Bytes, byte[]>>(
        Materialized.as(new ResponsiveSessionedStoreSupplier(params)),
        params.truncateChangelog()
    );
  }

  private ResponsiveStores() {
  }
}
