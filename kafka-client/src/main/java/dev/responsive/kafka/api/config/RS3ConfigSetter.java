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

import dev.responsive.kafka.api.config.RS3StoreParams.RS3KVStoreParams;
import dev.responsive.kafka.api.config.RS3StoreParams.RS3SessionStoreParams;
import dev.responsive.kafka.api.config.RS3StoreParams.RS3WindowStoreParams;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.SessionSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import java.util.Map;
import org.apache.kafka.common.Configurable;

public interface RS3ConfigSetter extends Configurable {

  /**
   * Selects the configs for a {@link org.apache.kafka.streams.state.KeyValueStore}
   * with the provided name and schema.
   *
   * @return the {@link RS3KVStoreParams configs} to apply for this store
   */
  default RS3KVStoreParams keyValueStoreConfig(String storeName, KVSchema schema) {
    return RS3KVStoreParams.defaults(schema);
  }

  /**
   * Selects the configs for a {@link org.apache.kafka.streams.state.WindowStore}
   * with the provided name and schema.
   *
   * @return the {@link RS3WindowStoreParams configs} to apply for this store
   */
  default RS3WindowStoreParams windowStoreConfig(String storeName, WindowSchema schema) {
    return RS3WindowStoreParams.defaults(schema);
  }

  /**
   * Selects the configs for a {@link org.apache.kafka.streams.state.SessionStore}
   * with the provided name and schema.
   *
   * @return the {@link RS3SessionStoreParams configs} to apply for this store
   */
  default RS3SessionStoreParams sessionStoreConfig(String storeName, SessionSchema schema) {
    return RS3SessionStoreParams.defaults(schema);
  }

  @Override
  default void configure(final Map<String, ?> configs) {
  }

}
