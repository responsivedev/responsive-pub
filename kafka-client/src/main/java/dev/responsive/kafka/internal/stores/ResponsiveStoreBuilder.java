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

package dev.responsive.kafka.internal.stores;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

public interface ResponsiveStoreBuilder<K, V, T extends StateStore> extends StoreBuilder<T> {

  StoreSupplier<?> storeSupplier();

  class ResponsiveKeyValueStoreBuilder<K, V, T> extends KeyValueStoreBuilder<K, V>
      implements ResponsiveStoreBuilder<K, V, KeyValueStore<K, V>>  {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public ResponsiveKeyValueStoreBuilder(
        final KeyValueBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
      super(storeSupplier, keySerde, valueSerde, time);
      this.storeSupplier = storeSupplier;
    }

    @Override
    public KeyValueBytesStoreSupplier storeSupplier() {
      return storeSupplier;
    }
  }

  class ResponsiveTimestampedKeyValueStoreBuilder<K, V, T>
      extends TimestampedKeyValueStoreBuilder<K, V>
      implements ResponsiveStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>  {

    private final KeyValueBytesStoreSupplier storeSupplier;

    public ResponsiveTimestampedKeyValueStoreBuilder(
        final KeyValueBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
      super(storeSupplier, keySerde, valueSerde, time);
      this.storeSupplier = storeSupplier;
    }

    @Override
    public KeyValueBytesStoreSupplier storeSupplier() {
      return storeSupplier;
    }
  }

  class ResponsiveWindowStoreBuilder<K, V, T> extends WindowStoreBuilder<K, V>
      implements ResponsiveStoreBuilder<K, V, WindowStore<K, V>>  {

    private final WindowBytesStoreSupplier storeSupplier;

    public ResponsiveWindowStoreBuilder(
        final WindowBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
      super(storeSupplier, keySerde, valueSerde, time);
      this.storeSupplier = storeSupplier;
    }

    @Override
    public WindowBytesStoreSupplier storeSupplier() {
      return storeSupplier;
    }
  }

  class ResponsiveTimestampedWindowStoreBuilder<K, V, T> extends TimestampedWindowStoreBuilder<K, V>
      implements ResponsiveStoreBuilder<K, V, TimestampedWindowStore<K, V>>  {

    private final WindowBytesStoreSupplier storeSupplier;

    public ResponsiveTimestampedWindowStoreBuilder(
        final WindowBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
      super(storeSupplier, keySerde, valueSerde, time);
      this.storeSupplier = storeSupplier;
    }

    @Override
    public WindowBytesStoreSupplier storeSupplier() {
      return storeSupplier;
    }
  }

  class ResponsiveSessionStoreBuilder<K, V, T> extends SessionStoreBuilder<K, V>
      implements ResponsiveStoreBuilder<K, V, SessionStore<K, V>>  {

    private final SessionBytesStoreSupplier storeSupplier;

    public ResponsiveSessionStoreBuilder(
        final SessionBytesStoreSupplier storeSupplier,
        final Serde<K> keySerde,
        final Serde<V> valueSerde,
        final Time time
    ) {
      super(storeSupplier, keySerde, valueSerde, time);
      this.storeSupplier = storeSupplier;
    }

    @Override
    public SessionBytesStoreSupplier storeSupplier() {
      return storeSupplier;
    }
  }
}
