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
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveKeyValueStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveTimestampedKeyValueStoreBuilder;
import java.lang.reflect.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

public class DelayedAsyncStoreBuilder<K, V, T extends StateStore>
    extends AbstractAsyncStoreBuilder<T> {

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

  @SuppressWarnings({"rawtypes"})
  private StateStore getKeyValueStore(final KeyValueStoreBuilder kvBuilder) {
    final KeyValueBytesStoreSupplier storeSupplier;
    if (kvBuilder instanceof ResponsiveStoreBuilder.ResponsiveKeyValueStoreBuilder) {
      storeSupplier = ((ResponsiveKeyValueStoreBuilder) kvBuilder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            KeyValueStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier =
            (KeyValueBytesStoreSupplier) storeSupplierField.get(kvBuilder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to retrieve store supplier for async "
                                            + "key-value store", e);
      }
    }
    return getKeyValueStore(storeSupplier, kvBuilder.time, kvBuilder.keySerde, kvBuilder.valueSerde);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getKeyValueStore(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Time time,
      final Serde keySerde,
      final Serde valueSerde
  ) {
    final KeyValueStore store = storeSupplier.get();

    return new MeteredKeyValueStore<>(
        wrapAsyncFlushingKV(
            maybeWrapCachingKV(
                maybeWrapLoggingKV(store))
        ),
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  @SuppressWarnings({"rawtypes"})
  private StateStore getTimestampedKeyValueStore(
      final TimestampedKeyValueStoreBuilder kvBuilder
  ) {
    final KeyValueBytesStoreSupplier storeSupplier;
    if (kvBuilder instanceof ResponsiveStoreBuilder.ResponsiveTimestampedKeyValueStoreBuilder) {
      storeSupplier = ((ResponsiveTimestampedKeyValueStoreBuilder) kvBuilder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            TimestampedKeyValueStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier = (KeyValueBytesStoreSupplier) storeSupplierField.get(kvBuilder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to build async key-value store", e);
      }
    }

    return getTimestampedKeyValueStore(
        storeSupplier,
        kvBuilder.time,
        kvBuilder.keySerde,
        kvBuilder.valueSerde
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getTimestampedKeyValueStore(
      final KeyValueBytesStoreSupplier storeSupplier,
      final Time time,
      final Serde keySerde,
      final Serde valueSerde
  ) {
    final KeyValueStore store = storeSupplier.get();

    return new MeteredTimestampedKeyValueStore<>(
        wrapAsyncFlushingKV(
            maybeWrapCachingKV(
                maybeWrapLoggingTimestampedKV(store))
        ),
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapCachingKV(
      final KeyValueStore<Bytes, byte[]> inner
  ) {
    if (!cachingEnabled()) {
      return inner;
    }
    return new CachingKeyValueStore(inner, true);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLoggingKV(
      final KeyValueStore<Bytes, byte[]> inner
  ) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingKeyValueBytesStore(inner);
  }

  private KeyValueStore<Bytes, byte[]> maybeWrapLoggingTimestampedKV(
      final KeyValueStore<Bytes, byte[]> inner
  ) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
  }

}