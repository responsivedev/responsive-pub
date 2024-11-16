/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.api.stores;

import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.DslSessionParams;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.DslWindowParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

public class ResponsiveDslStoreSuppliers implements DslStoreSuppliers {

  @Override
  public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams dslKeyValueParams) {
    return ResponsiveStores.keyValueStore(
        ResponsiveKeyValueParams.keyValue(dslKeyValueParams.name())
    );
  }

  @Override
  public WindowBytesStoreSupplier windowStore(final DslWindowParams dslWindowParams) {
    return ResponsiveStores.windowStoreSupplier(
        ResponsiveWindowParams.window(
            dslWindowParams.name(),
            dslWindowParams.windowSize(),
            dslWindowParams.retentionPeriod(),
            dslWindowParams.retainDuplicates()
        )
    );
  }

  @Override
  public SessionBytesStoreSupplier sessionStore(final DslSessionParams dslSessionParams) {
    return ResponsiveStores.sessionStoreSupplier(
        ResponsiveSessionParams.session(
            dslSessionParams.name(),
            dslSessionParams.retentionPeriod()
        )
    );
  }
}
