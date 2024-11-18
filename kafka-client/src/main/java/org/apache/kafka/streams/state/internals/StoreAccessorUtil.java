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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;

public class StoreAccessorUtil {

  @SuppressWarnings("rawtypes")
  public static StateSerdes<?, ?> extractKeyValueStoreSerdes(
      final StateStore store
  ) {
    if (store instanceof MeteredKeyValueStore) {
      return ((MeteredKeyValueStore) store).serdes;
    } else {
      throw new IllegalStateException("Attempted to extract serdes from store of type "
                                          + store.getClass().getName());
    }
  }
}
