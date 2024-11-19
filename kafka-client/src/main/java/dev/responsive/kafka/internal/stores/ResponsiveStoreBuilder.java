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

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;

public class ResponsiveStoreBuilder<K, V, T extends StateStore> implements StoreBuilder<T> {

  private final StoreType storeType;
  private final StoreSupplier<?> userStoreSupplier;
  private final StoreBuilder<T> userStoreBuilder;
  private final Serde<K> keySerde;
  // Note: the valueSerde is not necessary of type V, eg in case of timestamped stores
  private final Serde<?> valueSerde;
  private final Time time;

  public enum StoreType {
    KEY_VALUE,
    TIMESTAMPED_KEY_VALUE,
    WINDOW,
    TIMESTAMPED_WINDOW,
    SESSION
  }

  public ResponsiveStoreBuilder(
      final StoreType storeType,
      final StoreSupplier<?> userStoreSupplier,
      final StoreBuilder<T> userStoreBuilder,
      final Serde<K> keySerde,
      final Serde<?> valueSerde
  ) {
    // the time parameter only exists for Streams unit tests and in non-testing code
    // will always hard-code Time.SYSTEM
    this(
        storeType,
        userStoreSupplier,
        userStoreBuilder,
        keySerde,
        valueSerde,
        Time.SYSTEM
    );
  }

  private ResponsiveStoreBuilder(
      final StoreType storeType,
      final StoreSupplier<?> userStoreSupplier,
      final StoreBuilder<T> userStoreBuilder,
      final Serde<K> keySerde,
      final Serde<?> valueSerde,
      final Time time
  ) {
    this.storeType = storeType;
    this.userStoreSupplier = userStoreSupplier;
    this.userStoreBuilder = userStoreBuilder;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.time = time;
  }

  public StoreType storeType() {
    return storeType;
  }

  public StoreSupplier<?> storeSupplier() {
    return userStoreSupplier;
  }

  public Serde<K> keySerde() {
    return keySerde;
  }

  // For timestamped stores, this will be the serde for the inner value type
  // which will not be the same type as V, which is the store's actual V type
  // (and would actually be TimestampAndValue<VInner> for timestamped stores)
  @SuppressWarnings("unchecked")
  public <VInner> Serde<VInner> innerValueSerde() {
    return (Serde<VInner>) valueSerde;
  }

  public Time time() {
    return time;
  }

  @Override
  public StoreBuilder<T> withCachingEnabled() {
    userStoreBuilder.withCachingEnabled();
    return this;
  }

  @Override
  public StoreBuilder<T> withCachingDisabled() {
    userStoreBuilder.withCachingDisabled();
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
    userStoreBuilder.withLoggingEnabled(config);
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingDisabled() {
    userStoreBuilder.withLoggingDisabled();
    throw new UnsupportedOperationException(
        "Responsive stores are currently incompatible with disabling the changelog. "
            + "Please reach out to us to request this feature.");
  }

  @Override
  public T build() {
    return userStoreBuilder.build();
  }

  @Override
  public Map<String, String> logConfig() {
    return userStoreBuilder.logConfig();
  }

  @Override
  public boolean loggingEnabled() {
    return userStoreBuilder.loggingEnabled();
  }

  @Override
  public String name() {
    return userStoreBuilder.name();
  }

}
