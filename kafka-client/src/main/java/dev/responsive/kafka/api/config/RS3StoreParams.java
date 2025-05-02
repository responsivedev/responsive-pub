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

public abstract class RS3StoreParams {

  public static class RS3KVStoreParams extends RS3StoreParams {

    public static final Optional<Integer> DEFAULT_FACT_STORE_FILTER_BITS = Optional.of(20);

    private RS3KVStoreParams(final Optional<Integer> filterBitsPerKey) {
      super(filterBitsPerKey);
    }

    public static RS3KVStoreParams defaults(final KVSchema schema) {
      final Optional<Integer> defaultFilterBitsPerKey = schema == KVSchema.FACT
          ? DEFAULT_FACT_STORE_FILTER_BITS
          : Optional.empty();
      return new RS3KVStoreParams(defaultFilterBitsPerKey);
    }

    public RS3KVStoreParams withFilterBitsPerKey(final int filterBitsPerKey) {
      return new RS3KVStoreParams(Optional.of(filterBitsPerKey));
    }
  }

  public static class RS3WindowStoreParams extends RS3StoreParams {

    private RS3WindowStoreParams(final Optional<Integer> filterBitsPerKey) {
      super(filterBitsPerKey);
    }

    public static RS3WindowStoreParams defaults(final WindowSchema schema) {
      return new RS3WindowStoreParams(Optional.empty());
    }

    public RS3WindowStoreParams withFilterBitsPerKey(final int filterBitsPerKey) {
      return new RS3WindowStoreParams(Optional.of(filterBitsPerKey));
    }
  }

  public static class RS3SessionStoreParams extends RS3StoreParams {

    private RS3SessionStoreParams(final Optional<Integer> filterBitsPerKey) {
      super(filterBitsPerKey);
    }

    public static RS3SessionStoreParams defaults(final SessionSchema schema) {
      return new RS3SessionStoreParams(Optional.empty());
    }

    public RS3SessionStoreParams withFilterBitsPerKey(final int filterBitsPerKey) {
      return new RS3SessionStoreParams(Optional.of(filterBitsPerKey));
    }
  }

  private final Optional<Integer> filterBitsPerKey;

  private RS3StoreParams(final Optional<Integer> filterBitsPerKey) {
    this.filterBitsPerKey = filterBitsPerKey;
  }

  public Optional<Integer> filterBitsPerKey() {
    return filterBitsPerKey;
  }


}
