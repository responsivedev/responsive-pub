package dev.responsive.kafka.api;

import java.time.Duration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
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
public class ResponsiveStoreFactory {

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link KeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized(KeyValueStoreOptions)} instead.
   *
   * @param options the {@link KeyValueStoreOptions} to use
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(
      final KeyValueStoreOptions<K, V> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedKeyValueStore} and connect it via the Processor API. If using the DSL, use
   * {@link #materialized(KeyValueStoreOptions)} instead.
   *
   * @param options the {@link KeyValueStoreOptions} to use
   * @return a store builder that can be used to build a key-value store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(
      final KeyValueStoreOptions<K, V> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive {@link WindowStore}
   * and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized(WindowStoreOptions)} instead for most operators and
   * {@link #streamJoinedStore(String, JoinWindows)} for stream-stream joins specifically.
   *
   * @param options the {@link WindowStoreOptions} to use
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(
      final WindowStoreOptions<K, V> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link StoreBuilder} that can be used to build a Responsive
   * {@link TimestampedWindowStore} and connect it via the Processor API. If using the DSL, use
   * {@link #windowMaterialized(WindowStoreOptions)} instead for most operators and
   * {@link #streamJoinedStore(String, JoinWindows)} for stream-stream joins specifically.
   *
   * @param options the {@link WindowStoreOptions} to use
   * @return a store builder that can be used to build a window store with the given options
   *         that uses Responsive's storage for its backend
   */
  public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(
      final WindowStoreOptions<K, V> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link WindowBytesStoreSupplier} for a windowed Responsive state store specific to
   * stream-stream joins in the DSL. This should be set for both stores in the {@link StreamJoined}
   * configuration options passed in to the stream-stream join operator.
   *
   * @param name the name for this store
   * @param joinWindows the window configuration for this stream-stream join
   * @return a store supplier that can be used to build a window store for stream-stream joins
   *         with the given name that uses Responsive's storage for its backend
   */
  public static WindowBytesStoreSupplier streamJoinedStore(
      final String name,
      final JoinWindows joinWindows
  ) {
    final WindowStoreOptions<?, ?> options =
        WindowStoreOptions.withNameAndSize(name, Duration.ofMillis(joinWindows.size()))
            .withRetention(Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()))
            .withRetainDuplicates(true);
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link KeyValueStore}
   * and materialized in the DSL. If using the low-level Processor API, use
   * {@link #keyValueStoreBuilder(KeyValueStoreOptions)} instead.
   *
   * @param options the {@link KeyValueStoreOptions} to use
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(
      final KeyValueStoreOptions<?, ?> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  }

  /**
   * Create a {@link Materialized} that can be used to build a Responsive {@link WindowStore}
   * and materialized in the DSL for most operators. For stream-stream joins specifically, see
   * {@link #streamJoinedStore(String, JoinWindows)} instead. If using the low-level Processor API,
   * use {@link #windowStoreBuilder(WindowStoreOptions)}
   *
   * @param options the {@link WindowStoreOptions} to use
   * @return a Materialized configuration that can be used to build a key value store with the
   *         given options that uses Responsive's storage for its backend
   */
  public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> windowMaterialized(
      final WindowStoreOptions<?, ?> options
  ) {
    throw new UnsupportedOperationException("Not yet implemented: use ResponsiveDriver for now");
  };

}
