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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.StateStore;

public class MaterializedStoreFactoryUtil {

  public static <K, V, S extends StateStore> MaterializedInternal<K, V, S> getMaterialized(
      MaterializedStoreFactory<K, V, S> msf
  ) {
    return msf.materialized;
  }

}
