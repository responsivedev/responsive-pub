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

package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.internals.stores.AsyncFlushingKeyValueStore;
import dev.responsive.kafka.api.async.internals.stores.AsyncStoreBuilder;
import dev.responsive.kafka.api.async.internals.stores.StreamThreadFlushListeners;
import dev.responsive.kafka.api.async.internals.stores.StreamThreadFlushListeners.AsyncFlushListener;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Essentially a copy of the {@link TimestampedKeyValueStoreBuilder} class that
 * allows us to inject an additional layer, the {@link AsyncFlushingKeyValueStore}.
 * We also use this builder to coordinate between the async processor (which is
 * responsible for creating this builder) and the async flushing store (which is
 * created by this builder).
 */
public class AsyncTimestampedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>
    implements AsyncStoreBuilder<TimestampedKeyValueStore<K, V>> {

  private final KeyValueBytesStoreSupplier storeSupplier;
  private final boolean cachingEnabled;
  private final boolean loggingEnabled;

  // Since there is only one StoreBuilder instance for each store, it is used by all of the
  // StreamThreads in an app, and so we must account for which StreamThread is building
  // or accessing which stores
  private final Map<String, StreamThreadFlushListeners> streamThreadToFlushListeners =
      new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "checkstyle:linelength"})
  public AsyncTimestampedKeyValueStoreBuilder(
      final ResponsiveStoreBuilder<?, ?, ?> responsiveBuilder
  ) {
    this(
        (KeyValueBytesStoreSupplier) responsiveBuilder.storeSupplier(),
        ((ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>) responsiveBuilder).keySerde(),
        responsiveBuilder.innerValueSerde(),
        responsiveBuilder.time(),
        responsiveBuilder.cachingEnabled(),
        responsiveBuilder.loggingEnabled()
    );
  }

  private AsyncTimestampedKeyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final Time time,
      final boolean cachingEnabled,
      final boolean loggingEnabled
  ) {
    super(
        storeSupplier.name(),
        keySerde,
        valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
        time
    );
    this.storeSupplier = storeSupplier;
    this.cachingEnabled = cachingEnabled;
    this.loggingEnabled = loggingEnabled;
  }

  /**
   * Return the {@link StreamThreadFlushListeners} for this StreamThread,
   * creating/registering a new one if this is the first time we're seeing
   * the current StreamThread.
   * This should be a no-op if the builder has already registered this thread.
   */
  private StreamThreadFlushListeners getOrCreateFlushListeners(
      final String streamThreadName
  ) {
    return streamThreadToFlushListeners.computeIfAbsent(
        streamThreadName,
        k -> new StreamThreadFlushListeners(streamThreadName, name)
    );
  }

  @Override
  public void registerFlushListenerWithAsyncStore(
      final String streamThreadName,
      final int partition,
      final AsyncFlushListener processorFlushListener
  ) {
    final StreamThreadFlushListeners threadListeners =
        streamThreadToFlushListeners.get(streamThreadName);
    if (threadListeners == null) {
      throw new IllegalStateException("Unable to locate flush listener metadata "
                                          + "for the current StreamThread");
    }
    threadListeners.registerListenerForPartition(partition, processorFlushListener);
  }

  @Override
  public void unregisterFlushListenerForPartition(
      final String streamThreadName,
      final int partition
  ) {
    final StreamThreadFlushListeners threadListeners =
        streamThreadToFlushListeners.get(streamThreadName);

    if (threadListeners == null) {
      throw new IllegalStateException("Unable to locate flush listener metadata "
                                          + "for the current StreamThread");
    }

    threadListeners.unregisterListenerForPartition(partition);
  }

  @Override
  public TimestampedKeyValueStore<K, V> build() {
    final KeyValueStore<Bytes, byte[]> store = storeSupplier.get();
    if (!(store instanceof TimestampedBytesStore)) {
      throw new IllegalStateException("Timestamped store builder expects store supplier to provide "
                                          + "store that implements TimestampedBytesStore");
    }

    return new MeteredTimestampedKeyValueStore<>(
        wrapAsyncFlushing(
            maybeWrapCaching(
                maybeWrapLogging(store))
        ),
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  private KeyValueStore<Bytes, byte[]> wrapAsyncFlushing(final KeyValueStore<Bytes, byte[]> inner) {
    final StreamThreadFlushListeners threadFlushListeners =
        getOrCreateFlushListeners(Thread.currentThread().getName());

    return new AsyncFlushingKeyValueStore(inner, threadFlushListeners);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
    if (!cachingEnabled) {
      return inner;
    }
    return new CachingKeyValueStore(inner, true);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
    if (!loggingEnabled) {
      return inner;
    }
    return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
  }

}
