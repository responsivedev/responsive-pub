/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.api.stores;

import dev.responsive.kafka.internal.stores.ResponsiveMaterialized;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.StoreType;
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
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveKeyValueBytesStoreSupplier keyValueStore(
      final ResponsiveKeyValueParams params
  ) {
    return new ResponsiveKeyValueBytesStoreSupplier(params);
  }

  /**
   * See for example {@link Stores#inMemoryKeyValueStore(String)}
   *
   * @param name the store name
   * @return a supplier for a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveKeyValueBytesStoreSupplier keyValueStore(final String name) {
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
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveKeyValueBytesStoreSupplier factStore(final String name) {
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
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store that is compatible
   *        with the Responsive framework
   */
  public static <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        StoreType.KEY_VALUE,
        storeSupplier,
        Stores.keyValueStoreBuilder(storeSupplier, keySerde, valueSerde),
        keySerde,
        valueSerde
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
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store that is compatible
   *         with the Responsive framework
   */
  public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {

    if (storeSupplier instanceof ResponsiveKeyValueBytesStoreSupplier) {
      ((ResponsiveKeyValueBytesStoreSupplier) storeSupplier).asTimestamped();
    }

    return new ResponsiveStoreBuilder<>(
        StoreType.TIMESTAMPED_KEY_VALUE,
        storeSupplier,
        Stores.timestampedKeyValueStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde),
        keySerde,
        valueSerde
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link KeyValueStore}
   * and materialized in the DSL. If using the low-level Processor API, use
   * {@link #keyValueStoreBuilder} instead.
   *
   * @param params the store parameters
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final ResponsiveKeyValueParams params
  ) {
    return new ResponsiveMaterialized<>(
        Materialized.as(keyValueStore(params))
    );
  }

  //////////////////////////// Window Stores ////////////////////////////

  /**
   * See for example {@link Stores#inMemoryWindowStore(String, Duration, Duration, boolean)}
   *
   * @param params the {@link ResponsiveWindowParams} for this store
   *        use {@link ResponsiveWindowParams#window(String, Duration, Duration, boolean)} for
   *               windowed aggregations in the DSL or PAPI stores with update semantics
   *        use {@link ResponsiveWindowParams#streamStreamJoin(String, Duration)} for
   *               stream-stream joins in the DSL  or PAPI stores with duplicates semantics
   * @return a supplier for a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveWindowBytesStoreSupplier windowStoreSupplier(
      final ResponsiveWindowParams params
  ) {
    return new ResponsiveWindowBytesStoreSupplier(params);
  }

  /**
   * See for example {@link Stores#inMemoryWindowStore(String, Duration, Duration, boolean)}
   *
   * @param name the store name
   * @param retentionPeriod the retention period, must be greater than or equal to window size
   * @param windowSize the window size, must be greater than 0
   * @param retainDuplicates whether to retain duplicates vs overwrite records, this must be false
   *                         for all DSL operators except stream-stream joins which require true
   * @return a supplier for a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveWindowBytesStoreSupplier windowStoreSupplier(
      final String name,
      final Duration retentionPeriod,
      final Duration windowSize,
      final boolean retainDuplicates
  ) {
    if (windowSize.isNegative() || windowSize.isZero()) {
      throw new IllegalArgumentException("Window size cannot be negative or zero");
    }

    if (!retainDuplicates) {
      if (retentionPeriod.compareTo(windowSize) < 0) {
        throw new IllegalArgumentException("Retention period cannot be less than window size");
      }

      return new ResponsiveWindowBytesStoreSupplier(
          ResponsiveWindowParams.window(name, windowSize, retentionPeriod, retainDuplicates)
      );
    } else {
      if (!windowSize.equals(retentionPeriod)) {
        throw new IllegalArgumentException(
            "Retention period must be equal to window size for stream-stream join stores"
        );
      }

      return new ResponsiveWindowBytesStoreSupplier(
          ResponsiveWindowParams.streamStreamJoin(name, windowSize)
      );
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
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store that is compatible
   *         with the Responsive framework
   */
  public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(
      final WindowBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        StoreType.WINDOW,
        storeSupplier,
        Stores.windowStoreBuilder(storeSupplier, keySerde, valueSerde),
        keySerde,
        valueSerde
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
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store that is compatible
   *         with the Responsive framework
   */
  public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(
      final WindowBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        StoreType.TIMESTAMPED_WINDOW,
        storeSupplier,
        Stores.timestampedWindowStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde),
        keySerde,
        valueSerde
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link WindowStore}
   * and materialized in the DSL for most operators. If using the low-level Processor API,
   * use {@link #windowStoreBuilder}
   *
   * @param params the store parameters
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> windowMaterialized(
      final ResponsiveWindowParams params
  ) {
    return new ResponsiveMaterialized<>(
        Materialized.as(new ResponsiveWindowBytesStoreSupplier(params))
    );
  }

  /**
   * See for example {@link Stores#inMemorySessionStore(String, Duration)}
   *
   * @param params the {@link ResponsiveSessionParams} for this store
   * @return a supplier for a session store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static ResponsiveSessionBytesStoreSupplier sessionStoreSupplier(
      final ResponsiveSessionParams params
  ) {
    return new ResponsiveSessionBytesStoreSupplier(params);
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
   * @return a store builder that can be used to build a session store that is compatible
   *         with the Responsive framework
   */
  public static <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(
      final SessionBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    return new ResponsiveStoreBuilder<>(
        StoreType.SESSION,
        storeSupplier,
        Stores.sessionStoreBuilder(storeSupplier, keySerde, valueSerde),
        keySerde,
        valueSerde
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
    return new ResponsiveMaterialized<>(
        Materialized.as(new ResponsiveSessionBytesStoreSupplier(params))
    );
  }

  private ResponsiveStores() {
  }
}
