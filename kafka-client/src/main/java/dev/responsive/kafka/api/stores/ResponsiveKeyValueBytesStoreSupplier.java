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

package dev.responsive.kafka.api.stores;

import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveTimestampedKeyValueStore;
import java.util.Locale;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

  private final ResponsiveKeyValueParams params;

  private boolean isTimestamped;

  public ResponsiveKeyValueBytesStoreSupplier(final ResponsiveKeyValueParams params) {
    this.params = params;
  }

  public void asTimestamped() {
    this.isTimestamped = true;
  }

  @Override
  public String name() {
    return params.name().kafkaName();
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    if (isTimestamped) {
      return new ResponsiveTimestampedKeyValueStore(params);
    } else {
      return new ResponsiveKeyValueStore(params);
    }
  }

  @Override
  public String metricsScope() {
    return "responsive-" + params.schemaType().name().toLowerCase(Locale.ROOT);
  }
}