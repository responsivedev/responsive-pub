package dev.responsive.kafka.api;

import dev.responsive.kafka.store.ResponsiveMaterialized;
import dev.responsive.kafka.store.ResponsiveStoreBuilder;
import dev.responsive.utils.StoreUtil;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
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

  /**
   * See for example {@link Stores#inMemoryKeyValueStore(String)}
   *
   * @param name the store name
   * @return a supplier for a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static KeyValueBytesStoreSupplier keyValueStore(final String name) {
    return new ResponsiveKeyValueBytesStoreSupplier(name, false);
  }

  /**
   * See for example {@link Stores#persistentTimestampedKeyValueStore(String)}
   *
   * @param name the store name
   * @return a supplier for a timestamped key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static KeyValueBytesStoreSupplier timestampedKeyValueStore(final String name) {
    return new ResponsiveKeyValueBytesStoreSupplier(name, true);
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link KeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized(String)} instead.
   * <p>
   * See {@link Stores#keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
   *
   * @param storeSupplier the key-value store supplier
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
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
   * {@link TimestampedKeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized} instead.
   * <p>
   * See {@link Stores#timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
   *
   * @param storeSupplier the timestamped key-value store supplier
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
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
            valueSerde),
        false
    );
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
  public static WindowBytesStoreSupplier windowStoreSupplier(
      final String name,
      final Duration retentionPeriod,
      final Duration windowSize,
      final boolean retainDuplicates
  ) {
    final long retentionMs = StoreUtil.durationToMillis(retentionPeriod, "retentionPeriod");
    final long windowSizeMs = StoreUtil.durationToMillis(windowSize, "windowSize");
    if (windowSizeMs < 0) {
      throw new IllegalArgumentException("Window size cannot be zero");
    } else if (retentionMs < windowSizeMs) {
      throw new IllegalArgumentException("Retention period cannot be less than window size");
    }

    return new ResponsiveWindowedStoreSupplier(
        name,
        retentionMs,
        windowSizeMs,
        retainDuplicates
    );
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
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
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
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
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
            valueSerde),
        false
    );
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link KeyValueStore}
   * and materialized in the DSL. If using the low-level Processor API, use
   * {@link #keyValueStoreBuilder} instead.
   *
   * @param name the store name
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final String name
  ) {
    final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized =
        Materialized.as(new ResponsiveKeyValueBytesStoreSupplier(name, true));

    return new ResponsiveMaterialized<>(materialized, false);
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link WindowStore}
   * and materialized in the DSL for most operators. If using the low-level Processor API,
   * use {@link #windowStoreBuilder}
   *
   * @param name the store name
   * @param retentionMs the retention period in milliseconds
   * @param windowSize the window size in milliseconds
   * @param retainDuplicates whether to retain duplicates. Should be false for most operators
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> windowMaterialized(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized = Materialized.as(
        new ResponsiveWindowedStoreSupplier(name, retentionMs, windowSize, retainDuplicates)
    );

    return new ResponsiveMaterialized<>(
        materialized,
        false
    );
  }


  private ResponsiveStores() { }
}
