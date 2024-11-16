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

import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import dev.responsive.kafka.api.async.internals.stores.AsyncFlushingKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Essentially a copy of the {@link TimestampedKeyValueStoreBuilder} class that
 * allows us to inject an additional layer, the {@link AsyncFlushingKeyValueStore}.
 * We also use this builder to coordinate between the async processor (which is
 * responsible for creating this builder) and the async flushing store (which is
 * created by this builder).
 */
public class AsyncTimestampedKeyValueStoreBuilder<K, V>
    extends AbstractAsyncStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncTimestampedKeyValueStoreBuilder.class);
  private final KeyValueBytesStoreSupplier storeSupplier;

  @SuppressWarnings("unchecked")
  public AsyncTimestampedKeyValueStoreBuilder(
      final ResponsiveStoreBuilder<?, ?, ?> responsiveBuilder
  ) {
    this(
        (KeyValueBytesStoreSupplier) responsiveBuilder.storeSupplier(),
        (Serde<K>) responsiveBuilder.keySerde(),
        responsiveBuilder.innerValueSerde(),
        responsiveBuilder.time()
    );
  }

  public AsyncTimestampedKeyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final Time time
  ) {
    super(
        storeSupplier.name(),
        keySerde,
        valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
        time
    );
    this.storeSupplier = storeSupplier;
    LOG.debug("Created async timestamped-KV store builder with valueSerde = {}", valueSerde);
  }

  @Override
  public TimestampedKeyValueStore<K, V> build() {
    final KeyValueStore<Bytes, byte[]> store = storeSupplier.get();
    if (!(store instanceof TimestampedBytesStore)) {
      throw new IllegalStateException("Timestamped store builder expects store supplier to provide "
                                          + "store that implements TimestampedBytesStore");
    }

    return new MeteredTimestampedKeyValueStore<>(
        wrapAsyncFlushingKV(
            maybeWrapCaching(
                maybeWrapLogging(store))
        ),
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
    if (!cachingEnabled()) {
      return inner;
    }
    return new CachingKeyValueStore(inner, true);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
  }

}
