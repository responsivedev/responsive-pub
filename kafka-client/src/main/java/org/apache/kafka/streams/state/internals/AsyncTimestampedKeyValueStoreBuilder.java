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

import dev.responsive.kafka.api.async.internals.AsyncCachedStateStore.AsyncCachingKeyValueStore;
import dev.responsive.kafka.api.async.internals.AsyncStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;


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

    public AsyncTimestampedKeyValueStoreBuilder(
        ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> responsiveBuilder
    ) {
      this(
          responsiveBuilder,
          (KeyValueBytesStoreSupplier) responsiveBuilder.storeSupplier(),
          responsiveBuilder.keySerde(),
          responsiveBuilder.valueSerde(),
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
    public TimestampedKeyValueStore<K, V> build() {
      return new MeteredTimestampedKeyValueStore<>(
          maybeWrapCaching(maybeWrapLogging(storeSupplier.get())),
          storeSupplier.metricsScope(),
          time,
          keySerde,
          valueSerde
      );
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
      if (!storeBuilder.cachingEnabled()) {
        return inner;
      }
      return new AsyncCachingKeyValueStore(new CachingKeyValueStore(inner, true));
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
      if (!storeBuilder.loggingEnabled()) {
        return inner;
      }
      return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
    }
}
