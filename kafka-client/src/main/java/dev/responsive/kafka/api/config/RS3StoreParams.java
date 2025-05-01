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
import java.util.Optional;

public class RS3StoreParams {

  public static final Optional<Integer> DEFAULT_FACT_STORE_FILTER_BITS = Optional.of(20);

  private final Optional<Integer> filterBitsPerKey;

  private RS3StoreParams(final Optional<Integer> filterBitsPerKey) {
    this.filterBitsPerKey = filterBitsPerKey;
  }

  public static RS3StoreParams defaultKV(final KVSchema schema) {
    final Optional<Integer> defaultFilterBitsPerKey = schema == KVSchema.FACT
        ? DEFAULT_FACT_STORE_FILTER_BITS
        : Optional.empty();
    return new RS3StoreParams(defaultFilterBitsPerKey);
  }

  public static RS3StoreParams defaultWindow(final WindowSchema schema) {
    return new RS3StoreParams(Optional.empty());
  }

  public static RS3StoreParams defaultSession(final SessionSchema schema) {
    return new RS3StoreParams(Optional.empty());
  }

  public RS3StoreParams withFilterBitsPerKey(final int filterBitsPerKey) {
    return new RS3StoreParams(Optional.of(filterBitsPerKey));
  }

  public Optional<Integer> filterBitsPerKey() {
    return filterBitsPerKey;
  }
}
