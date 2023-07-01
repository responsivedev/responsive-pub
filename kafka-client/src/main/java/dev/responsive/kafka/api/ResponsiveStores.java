package dev.responsive.kafka.api;

import dev.responsive.kafka.store.ResponsiveMaterialized;
import dev.responsive.kafka.store.ResponsiveStoreBuilder;
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
 *
 * See {@link org.apache.kafka.streams.state.Stores} for instructions on how to plug in custom
 * state stores and configure them.
 */
public final class ResponsiveStores {

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link KeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized(String)} instead.
   *
   * @param name the store name
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(
      final String name,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    final KeyValueBytesStoreSupplier storeSupplier =
        new ResponsiveKeyValueBytesStoreSupplier(name, false);

    return new ResponsiveStoreBuilder<>(
        Stores.keyValueStoreBuilder(storeSupplier, keySerde, valueSerde),
        false
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedKeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized} instead.
   *
   * @param name the store name
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(
      final String name,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {

    return new ResponsiveStoreBuilder<>(
        Stores.timestampedKeyValueStoreBuilder(
            new ResponsiveKeyValueBytesStoreSupplier(name, true),
            keySerde,
            valueSerde),
        false
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive {@link WindowStore}
   * and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized} instead.
   *
   * @param name the store name
   * @param retentionMs the retention period in milliseconds
   * @param windowSize the window size in milliseconds
   * @param retainDuplicates whether to retain duplicates. Should be false for most operators
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    final WindowBytesStoreSupplier storeSupplier = new ResponsiveWindowedStoreSupplier(
        name,
        retentionMs,
        windowSize,
        retainDuplicates
    );

    return new ResponsiveStoreBuilder<>(
        Stores.windowStoreBuilder(storeSupplier, keySerde, valueSerde),
        false
    );
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedWindowStore} and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized} instead.
   *
   * @param name the store name
   * @param retentionMs the retention period in milliseconds
   * @param windowSize the window size in milliseconds
   * @param retainDuplicates whether to retain duplicates. Should be false for most operators
   * @param keySerde the key serde. If null, the default.key.serde config will be used
   * @param valueSerde the value serde. If null, the default.value.serde config will be used
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(
      final String name,
      final long retentionMs,
      final long windowSize,
      final boolean retainDuplicates,
      final Serde<K> keySerde,
      final Serde<V> valueSerde
  ) {
    final WindowBytesStoreSupplier storeSupplier = new ResponsiveWindowedStoreSupplier(
        name,
        retentionMs,
        windowSize,
        retainDuplicates
    );

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
