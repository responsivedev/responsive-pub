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
import java.lang.reflect.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedBytesStore;

public class DelayedAsyncStoreBuilder<K, V, T extends StateStore>
    extends AbstractAsyncStoreBuilder<K, V, T> {

  private final StoreBuilder<T> inner;
  private StoreBuilder<T> innerResolved;

  public DelayedAsyncStoreBuilder(final StoreBuilder<T> inner) {
    super(inner.name());
    this.inner = inner;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public T build() {
    if (innerResolved == null) {
      if (inner instanceof FactoryWrappingStoreBuilder) {
        innerResolved = ((FactoryWrappingStoreBuilder) inner).resolveStoreBuilder();
      } else {
        innerResolved = inner;
      }
    }

    if (innerResolved instanceof KeyValueStoreBuilder) {
      return (T) getKeyValueStore((KeyValueStoreBuilder) innerResolved);
    } else if (innerResolved instanceof TimestampedKeyValueStoreBuilder) {
      return (T) getTimestampedKeyValueStore((TimestampedKeyValueStoreBuilder) innerResolved);
    } else {
      throw new UnsupportedOperationException("Other store types not yet supported");
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getKeyValueStore(final KeyValueStoreBuilder kvBuilder) {
    try {
      final Field storeSupplierField = KeyValueStoreBuilder.class.getDeclaredField(
          "storeSupplier");

      // Set the accessibility as true
      storeSupplierField.setAccessible(true);

      // Store the value of private field in variable
      final KeyValueBytesStoreSupplier storeSupplier =
          (KeyValueBytesStoreSupplier) storeSupplierField.get(kvBuilder);

      final KeyValueStore store = storeSupplier.get();

      return new MeteredKeyValueStore<>(
          wrapAsyncFlushingKV(
              maybeWrapCachingKV(
                  maybeWrapLoggingKV(store))
          ),
          storeSupplier.metricsScope(),
          kvBuilder.time,
          kvBuilder.keySerde,
          kvBuilder.valueSerde
      );
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to build async key-value store", e);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getTimestampedKeyValueStore(
      final TimestampedKeyValueStoreBuilder kvBuilder
  ) {
    try {
      final Field storeSupplierField = TimestampedKeyValueStoreBuilder.class.getDeclaredField(
          "storeSupplier");

      // Set the accessibility as true
      storeSupplierField.setAccessible(true);

      // Store the value of private field in variable
      final KeyValueBytesStoreSupplier storeSupplier =
          (KeyValueBytesStoreSupplier) storeSupplierField.get(kvBuilder);

      final KeyValueStore store = storeSupplier.get();

      return new MeteredTimestampedKeyValueStore<>(
          wrapAsyncFlushingKV(
              maybeWrapCachingKV(
                  maybeWrapLoggingTimestampedKV(store))
          ),
          storeSupplier.metricsScope(),
          kvBuilder.time,
          Serdes.String(),
          kvBuilder.valueSerde
      );
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to build async key-value store", e);
    }
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapCachingKV(final KeyValueStore<Bytes, byte[]> inner) {
    if (!cachingEnabled()) {
      return inner;
    }
    return new CachingKeyValueStore(inner, true);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLoggingKV(final KeyValueStore<Bytes, byte[]> inner) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingKeyValueBytesStore(inner);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLoggingTimestampedKV(final KeyValueStore<Bytes, byte[]> inner) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
  }

}