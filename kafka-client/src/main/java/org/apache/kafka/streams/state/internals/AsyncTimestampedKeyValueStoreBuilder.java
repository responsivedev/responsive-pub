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

package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import dev.responsive.kafka.api.async.internals.stores.AsyncFlushingKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import java.lang.reflect.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
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

  public static <K, V> AsyncTimestampedKeyValueStoreBuilder<K, V> getAsyncBuilder(
      final TimestampedKeyValueStoreBuilder<K, V> storeBuilder
  ) {
    try {
      final Field storeSupplierField = TimestampedKeyValueStoreBuilder.class.getDeclaredField(
          "storeSupplier");

      // Set the accessibility as true
      storeSupplierField.setAccessible(true);

      // Store the value of private field in variable
      final KeyValueBytesStoreSupplier storeSupplier =
          (KeyValueBytesStoreSupplier) storeSupplierField.get(storeBuilder);

      return new AsyncTimestampedKeyValueStoreBuilder<>(
          storeSupplier,
          storeBuilder.keySerde,
          (ValueAndTimestampSerde<V>) storeBuilder.valueSerde,
          storeBuilder.time
      );
    } catch (final Exception e) {
      LOG.error("Unable to retrieve async store fields", e);
      throw new IllegalStateException("Can't build async stores", e);
    }
  }

  private AsyncTimestampedKeyValueStoreBuilder(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Serde<K> keySerde,
      final ValueAndTimestampSerde<V> valueSerde,
      final Time time
  ) {
    super(
        storeSupplier.name()
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
