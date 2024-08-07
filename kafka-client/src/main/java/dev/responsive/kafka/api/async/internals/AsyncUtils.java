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

import static dev.responsive.kafka.api.async.internals.AsyncThreadPool.ASYNC_THREAD_NAME;

import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreBuilder.StoreType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AsyncKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.AsyncTimestampedKeyValueStoreBuilder;

public class AsyncUtils {

  /**
   * @return true if the {@code threadName} is a StreamThread
   */
  public static boolean isStreamThread(
      final String threadName,
      final String streamThreadName
  ) {
    return threadName.equals(streamThreadName);
  }

  /**
   * @return true if the {@code threadName} is an AsyncThread
   */
  public static boolean isAsyncThread(
      final String threadName,
      final String streamThreadName
  ) {
    return threadName.startsWith(streamThreadName) && threadName.contains(ASYNC_THREAD_NAME);
  }

  /**
   * @return true if the {@code threadName} is either a StreamThread or an AsyncThread
   */
  public static boolean isStreamThreadOrAsyncThread(
      final String threadName,
      final String streamThreadName
  ) {
    return isAsyncThread(threadName, streamThreadName)
        || isStreamThread(threadName, streamThreadName);
  }

  public static Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> initializeAsyncBuilders(
      final Set<StoreBuilder<?>> userConnectedStores
  ) {
    if (userConnectedStores == null || userConnectedStores.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> asyncStoreBuilders = new HashMap<>();
    for (final StoreBuilder<?> builder : userConnectedStores) {
      final String storeName = builder.name();
      if (builder instanceof ResponsiveStoreBuilder) {
        final ResponsiveStoreBuilder<?, ?, ?> responsiveBuilder =
            (ResponsiveStoreBuilder<?, ?, ?>) builder;

        final StoreType storeType = responsiveBuilder.storeType();

        final AbstractAsyncStoreBuilder<?, ?, ?> storeBuilder;
        if (storeType.equals(StoreType.TIMESTAMPED_KEY_VALUE)) {
          storeBuilder = new AsyncTimestampedKeyValueStoreBuilder<>(responsiveBuilder);
        } else if (storeType.equals(StoreType.KEY_VALUE)) {
          storeBuilder = new AsyncKeyValueStoreBuilder<>(responsiveBuilder);
        } else {
          throw new UnsupportedOperationException(
              "Only key-value stores are supported by async processors at this time");
        }

        asyncStoreBuilders.put(
            storeName,
            storeBuilder
        );

      } else {
        throw new IllegalStateException(String.format(
            "Detected the StoreBuilder for %s was not created via the ResponsiveStores factory, "
                + "please ensure that all store builders and suppliers are provided through the "
                + "appropriate API from ResponsiveStores", storeName));
      }
    }
    return asyncStoreBuilders;
  }

  /**
   * Generates a consistent hashCode for the given {@link ProcessorRecordContext} by
   * This workaround is required due to the actual ProcessorRecordContext class throwing
   * an exception in its #hashCode implementation. This override is intended to discourage the use
   * of this class in hash-based collections, which in turn is because one of its fields,
   * {@link Headers}, is mutable and therefore the hashCode would be susceptible to
   * outside modification.
   * <p>
   * If the headers are guaranteed to be safe-guarded and made effectively immutable,
   * then they are safe to include in the hashCode. If no protections around the headers
   * exist, they should be left out of the hashCode computation. Use the {@code includeHeaders}
   * parameter to hash the headers or exclude them from the result.
   */
  public static int processorRecordContextHashCode(
      final ProcessorRecordContext recordContext,
      final boolean includeHeaders
  ) {
    int result = (int) (recordContext.timestamp() ^ (recordContext.timestamp() >>> 32));
    result = 31 * result + (int) (recordContext.offset() ^ (recordContext.offset() >>> 32));
    result = 31 * result + (recordContext.topic() != null ? recordContext.topic().hashCode() : 0);
    result = 31 * result + recordContext.partition();

    if (includeHeaders) {
      result = 31 * result + recordContext.headers().hashCode();
    }

    return result;
  }

}
