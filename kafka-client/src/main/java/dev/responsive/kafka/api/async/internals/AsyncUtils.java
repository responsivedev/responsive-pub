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

package dev.responsive.kafka.api.async.internals;

import static dev.responsive.kafka.api.async.internals.AsyncThreadPool.ASYNC_THREAD_NAME;
import static dev.responsive.kafka.internal.utils.Utils.isExecutingOnStreamThread;

import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.DelayedAsyncStoreBuilder;

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

  public static Map<String, AbstractAsyncStoreBuilder<?>> initializeAsyncBuilders(
      final Set<StoreBuilder<?>> userConnectedStores
  ) {
    if (userConnectedStores == null || userConnectedStores.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, AbstractAsyncStoreBuilder<?>> asyncStoreBuilders = new HashMap<>();
    for (final StoreBuilder<?> builder : userConnectedStores) {
      final String storeName = builder.name();

      asyncStoreBuilders.put(
          storeName,
          new DelayedAsyncStoreBuilder<>(builder)
      );

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
