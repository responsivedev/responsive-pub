/*
 *
 *  Copyright 2025 Responsive Computing, Inc.
 *
 *  This source code is licensed under the Responsive Business Source License Agreement v1.0
 *  available at:
 *
 *  https://www.responsive.dev/legal/responsive-bsl-10
 *
 *  This software requires a valid Commercial License Key for production use. Trial and commercial
 *  licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.api.config;

import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.SessionSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;

public interface RS3ConfigSetter extends Configurable {

  /**
   * Sets the configs for a {@link org.apache.kafka.streams.state.KeyValueStore}
   * with the provided name and schema. Can return {@link Optional#empty()} to defer to
   * the default settings for the given store type.
   *
   * @return the configs to override for this store
   */
  RS3StoreParams keyValueStoreConfig(String storeName, KVSchema schema);

  /**
   * Sets the configs for a {@link org.apache.kafka.streams.state.WindowStore}
   * with the provided name and schema. Can return {@link Optional#empty()} to defer to
   * the default settings for the given store type.
   *
   * @return the configs to override for this store
   */
  RS3StoreParams windowStoreConfig(String storeName, WindowSchema schema);

  /**
   * Sets the configs for a {@link org.apache.kafka.streams.state.SessionStore}
   * with the provided name and schema. Can return {@code super.sessionStoreConfig} to defer to
   * the default settings for the given store type.
   *
   * @return the configs to override for this store
   */
  RS3StoreParams sessionStoreConfig(String storeName, SessionSchema schema);

  @Override
  default void configure(final Map<String, ?> configs) {
  }

  class DefaultRS3ConfigSetter implements RS3ConfigSetter {

    @Override
    public RS3StoreParams keyValueStoreConfig(final String storeName, final KVSchema schema) {
      return RS3StoreParams.defaultKV(schema);
    }

    @Override
    public RS3StoreParams windowStoreConfig(final String storeName, final WindowSchema schema) {
      return RS3StoreParams.defaultWindow(schema);
    }

    @Override
    public RS3StoreParams sessionStoreConfig(final String storeName, final SessionSchema schema) {
      return RS3StoreParams.defaultSession(schema);
    }

  }
}
