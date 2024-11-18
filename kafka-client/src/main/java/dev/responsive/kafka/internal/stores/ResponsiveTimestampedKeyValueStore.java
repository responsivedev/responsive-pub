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

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import org.apache.kafka.streams.state.TimestampedBytesStore;

public class ResponsiveTimestampedKeyValueStore
    extends ResponsiveKeyValueStore implements TimestampedBytesStore {


  public ResponsiveTimestampedKeyValueStore(
      final ResponsiveKeyValueParams params
  ) {
    super(params);
  }

  public ResponsiveTimestampedKeyValueStore(
      final ResponsiveKeyValueParams params,
      final KVOperationsProvider opsProvider
  ) {
    super(params, opsProvider);
  }
}
