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
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveSessionStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveTimestampedKeyValueStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveTimestampedWindowStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.ResponsiveWindowStoreBuilder;
import java.lang.reflect.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

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
    } else if (innerResolved instanceof WindowStoreBuilder) {
      return (T) getWindowStore((WindowStoreBuilder) innerResolved);
    } else if (innerResolved instanceof TimestampedWindowStoreBuilder) {
      return (T) getTimestampedWindowStore((TimestampedWindowStoreBuilder) innerResolved);
    } else if (innerResolved instanceof SessionStoreBuilder) {
      return (T) getSessionStore((SessionStoreBuilder) innerResolved);
    } else {
      throw new UnsupportedOperationException("Other store types not yet supported");
    }
  }

  @SuppressWarnings({"rawtypes"})
  private StateStore getKeyValueStore(final KeyValueStoreBuilder builder) {
    final KeyValueBytesStoreSupplier storeSupplier;
    if (builder instanceof ResponsiveStoreBuilder.ResponsiveKeyValueStoreBuilder) {
      storeSupplier = ((ResponsiveKeyValueStoreBuilder) builder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            KeyValueStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier =
            (KeyValueBytesStoreSupplier) storeSupplierField.get(builder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to retrieve store supplier for async "
                                            + "key-value store", e);
      }
    }
    return getKeyValueStore(storeSupplier, builder.time, builder.keySerde, builder.valueSerde);
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
      final TimestampedKeyValueStoreBuilder builder
  ) {
    final KeyValueBytesStoreSupplier storeSupplier;
    if (builder instanceof ResponsiveStoreBuilder.ResponsiveTimestampedKeyValueStoreBuilder) {
      storeSupplier = ((ResponsiveTimestampedKeyValueStoreBuilder) builder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            TimestampedKeyValueStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier = (KeyValueBytesStoreSupplier) storeSupplierField.get(builder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to build async timestamped key-value store", e);
      }
    }

    return getTimestampedKeyValueStore(
        storeSupplier,
        builder.time,
        builder.keySerde,
        builder.valueSerde
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

  @SuppressWarnings({"rawtypes"})
  private StateStore getWindowStore(final WindowStoreBuilder builder) {
    final WindowBytesStoreSupplier storeSupplier;
    if (builder instanceof ResponsiveWindowStoreBuilder) {
      storeSupplier = ((ResponsiveWindowStoreBuilder) builder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            WindowStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier =
            (WindowBytesStoreSupplier) storeSupplierField.get(builder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to retrieve store supplier for async "
                                            + "window store", e);
      }
    }
    return getWindowStore(
        storeSupplier,
        builder.time,
        builder.keySerde,
        builder.valueSerde
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getWindowStore(
      final WindowBytesStoreSupplier storeSupplier,
      final Time time,
      final Serde keySerde,
      final Serde valueSerde
  ) {
    final WindowStore store = storeSupplier.get();

    final long windowSize = storeSupplier.windowSize();
    return new MeteredWindowStore<>(
        wrapAsyncFlushingWindow(
            maybeWrapCachingWindow(
                maybeWrapLoggingWindow(store, storeSupplier.retainDuplicates()),
                windowSize,
                storeSupplier.segmentIntervalMs())
        ),
        windowSize,
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  @SuppressWarnings({"rawtypes"})
  private StateStore getTimestampedWindowStore(
      final TimestampedWindowStoreBuilder builder
  ) {
    final WindowBytesStoreSupplier storeSupplier;
    if (builder instanceof ResponsiveTimestampedWindowStoreBuilder) {
      storeSupplier = ((ResponsiveTimestampedWindowStoreBuilder) builder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            TimestampedWindowStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier = (WindowBytesStoreSupplier) storeSupplierField.get(builder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to build async timestamped window store", e);
      }
    }

    return getTimestampedWindowStore(
        storeSupplier,
        builder.time,
        builder.keySerde,
        builder.valueSerde
    );
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getTimestampedWindowStore(
      final WindowBytesStoreSupplier storeSupplier,
      final Time time,
      final Serde keySerde,
      final Serde valueSerde
  ) {
    final WindowStore store = storeSupplier.get();

    final long windowSize = storeSupplier.windowSize();
    return new MeteredTimestampedWindowStore<>(
        wrapAsyncFlushingWindow(
            maybeWrapCachingWindow(
                maybeWrapLoggingTimestampedWindow(store, storeSupplier.retainDuplicates()),
                windowSize,
                storeSupplier.segmentIntervalMs())
        ),
        windowSize,
        storeSupplier.metricsScope(),
        time,
        keySerde,
        valueSerde
    );
  }

  @SuppressWarnings({"rawtypes"})
  private StateStore getSessionStore(final SessionStoreBuilder builder) {
    final SessionBytesStoreSupplier storeSupplier;
    if (builder instanceof ResponsiveSessionStoreBuilder) {
      storeSupplier = ((ResponsiveSessionStoreBuilder) builder).storeSupplier();
    } else {
      try {
        final Field storeSupplierField =
            SessionStoreBuilder.class.getDeclaredField("storeSupplier");
        storeSupplierField.setAccessible(true);

        storeSupplier =
            (SessionBytesStoreSupplier) storeSupplierField.get(builder);
      } catch (final Exception e) {
        throw new IllegalStateException("Failed to retrieve store supplier for async "
                                            + "session store", e);
      }
    }
    return getSessionStore(
        storeSupplier,
        builder.time,
        builder.keySerde,
        builder.valueSerde
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private StateStore getSessionStore(
      final SessionBytesStoreSupplier storeSupplier,
      final Time time,
      final Serde keySerde,
      final Serde valueSerde
  ) {
    final SessionStore store = storeSupplier.get();

    return new MeteredSessionStore<>(
        wrapAsyncFlushingSession(
            maybeWrapCachingSession(
                maybeWrapLoggingSession(store),
                storeSupplier.segmentIntervalMs())
        ),
        storeSupplier.metricsScope(),
        keySerde,
        valueSerde,
        time
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

  private WindowStore<Bytes, byte[]> maybeWrapCachingWindow(
      final WindowStore<Bytes, byte[]> inner,
      final long windowSize,
      final long segmentInterval
  ) {
    if (!cachingEnabled()) {
      return inner;
    }
    return new CachingWindowStore(inner, windowSize, segmentInterval);
  }

  private WindowStore<Bytes, byte[]> maybeWrapLoggingWindow(
      final WindowStore<Bytes, byte[]> inner,
      final boolean retainDuplicates
  ) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingWindowBytesStore(
        inner,
        retainDuplicates,
        WindowKeySchema::toStoreKeyBinary
    );
  }

  private WindowStore<Bytes, byte[]> maybeWrapLoggingTimestampedWindow(
      final WindowStore<Bytes, byte[]> inner,
      final boolean retainDuplicates
  ) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingTimestampedWindowBytesStore(inner, retainDuplicates);
  }

  private SessionStore<Bytes, byte[]> maybeWrapCachingSession(
      final SessionStore<Bytes, byte[]> inner,
      final long segmentInterval
  ) {
    if (!cachingEnabled()) {
      return inner;
    }
    return new CachingSessionStore(inner, segmentInterval);
  }

  private SessionStore<Bytes, byte[]> maybeWrapLoggingSession(
      final SessionStore<Bytes, byte[]> inner
  ) {
    if (!loggingEnabled()) {
      return inner;
    }
    return new ChangeLoggingSessionBytesStore(inner);
  }

}