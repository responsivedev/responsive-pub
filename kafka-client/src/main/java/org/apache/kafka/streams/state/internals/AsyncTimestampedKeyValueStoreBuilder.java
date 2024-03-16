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

import dev.responsive.kafka.api.async.internals.stores.AsyncProcessorFlushers.AsyncFlushListener;
import dev.responsive.kafka.api.async.internals.stores.AsyncStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;


// TODO:
//  1) make sure we're resolving serdes correctly in terms of raw vs timestamped values
//  2) test with passing in null serdes to builder
public class AsyncTimestampedKeyValueStoreBuilder<K, V> extends TimestampedKeyValueStoreBuilder<K, V>
    implements AsyncStoreBuilder<TimestampedKeyValueStore<K, V>> {

  private final KeyValueBytesStoreSupplier storeSupplier;
  private final ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> storeBuilder;
  private final Serde<K> keySerde;
  private final ValueAndTimestampSerde<V> valueSerde;
  private final Time time;

  // We need to maintain a reference to the method that will flush the async buffers and block
  // on their
  private final Map<String, AsyncFlushingKeyValueStore> streamThreadToFlushCallback = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  public AsyncTimestampedKeyValueStoreBuilder(
      final ResponsiveStoreBuilder<?, ?, ?> responsiveBuilder
  ) {
    this(
        (ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>) responsiveBuilder,
        (KeyValueBytesStoreSupplier) responsiveBuilder.storeSupplier(),
        ((ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>) responsiveBuilder).keySerde(),
        ((ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>) responsiveBuilder).valueSerde(),
        responsiveBuilder.time()
    );
  }

  private AsyncTimestampedKeyValueStoreBuilder(
      final ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> storeBuilder,
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final Time time
  ) {
    super(storeSupplier, keySerde, valueSerde, time);
    this.storeSupplier = storeSupplier;
    this.storeBuilder = storeBuilder;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde);
    this.time = time;
  }

  @Override
  public void registerNewProcessorForThread(
      final String streamThreadName,
      final int partition,
      final AsyncFlushListener processorFlushListener
  ) {

  }

  // When a StreamThread creates a newly-assigned tasks, it firsts builds the subtopology
  // which always starts by building the Processor instance itself (from ProcessorSupplier#get)
  // and then goes on to build any state stores, ie invoking this call
  // This means we can rely on the Processor to
    @Override
    public TimestampedKeyValueStore<K, V> build() {
      return new MeteredTimestampedKeyValueStore<>(
          maybeWrapCaching(maybeWrapLogging(storeSupplier.get())),
          storeSupplier.metricsScope(),
          time,
          keySerde,
          valueSerde
      );
    }

  /**
   * This is somewhat hacky, but because we hook into the StreamThread's commit
   * via the {@link CachedStateStore#flushCache()} API, we need
   */
  private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
      if (!storeBuilder.cachingEnabled()) {
        return inner;
      }
      return new AsyncFlushingKeyValueStore(inner, true);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
      if (!storeBuilder.loggingEnabled()) {
        return inner;
      }
      return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
    }

}
