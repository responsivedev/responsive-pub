/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals;

import dev.responsive.kafka.api.async.internals.stores.AsyncStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.StoreType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AsyncTimestampedKeyValueStoreBuilder;

public class Utils {

  public static Set<AsyncStoreBuilder<?>> initializeAsyncBuilders(
      final Set<StoreBuilder<?>> userConnectedStores
  ) {
    validateStoresConnectedToUserProcessor(userConnectedStores);

    final Map<String, AsyncStoreBuilder<?>> asyncStoreBuilders = new HashMap<>();
    for (final StoreBuilder<?> builder : userConnectedStores) {
      final String storeName = builder.name();
      if (builder instanceof ResponsiveStoreBuilder) {
        final ResponsiveStoreBuilder<?, ?, ?> responsiveBuilder = (ResponsiveStoreBuilder<?, ?, ?>) builder;

        final StoreType storeType = responsiveBuilder.storeType();

        if (storeType.equals(StoreType.TIMESTAMPED_KEY_VALUE)) {
          final AsyncStoreBuilder<?> storeBuilder =
              new AsyncTimestampedKeyValueStoreBuilder<>(
                  responsiveBuilder
              );

          asyncStoreBuilders.put(
              storeName,
              storeBuilder
          );
          storeBuilder.maybeRegisterNewStreamThread(Thread.currentThread().getName());

        } else {
          throw new UnsupportedOperationException("Only timestamped key-value stores are "
                                                      + "supported by async processors at this time");
        }
      } else {
        throw new IllegalStateException(String.format(
            "Detected the StoreBuilder for %s was not created via the ResponsiveStores factory, "
                + "please ensure that all store builders and suppliers are provided through the "
                + "appropriate API from ResponsiveStores", storeName));
      }
    }
    return asyncStoreBuilders;
  }

  private static void validateStoresConnectedToUserProcessor(
      final Set<StoreBuilder<?>> userConnectedStores
  ) {
    if (userConnectedStores == null || userConnectedStores.isEmpty()) {
      throw new UnsupportedOperationException("Async processing currently requires "
                                                  + "at least one state store be connected to the async processor, and that "
                                                  + "stores be connected by implementing the #stores method in your processor supplier");
    }
  }

}
