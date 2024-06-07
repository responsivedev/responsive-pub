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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Super hacky way to communicate whether async processing is enabled based on the configured
 * async thread pool size.
 * We have to use a static map to get this information from the ResponsiveConfig passed into
 * ResponsiveKafkaStreams to the async processor supplier *before* any state stores are built.
 * Unfortunately Kafka Streams does not pass in the app configs to any processor, supplier, or
 * state store constructors -- configs only become available to these elements when #init is called.
 */
public final class AsyncProcessingGate {

  private static final AtomicBoolean ASYNC_ENABLED = new AtomicBoolean(false);

  public static void maybeEnableAsyncProcessing(final int numAsyncThreads) {
    if (numAsyncThreads > 0) {
      ASYNC_ENABLED.set(true);
    }
  }

  public static void closeAsyncProcessing() {
    ASYNC_ENABLED.set(false);
  }

  public static boolean asyncEnabled() {
    return ASYNC_ENABLED.get();
  }

}
