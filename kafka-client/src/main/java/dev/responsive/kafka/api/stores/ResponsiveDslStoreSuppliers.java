/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
