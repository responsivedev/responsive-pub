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

package dev.responsive.kafka.internal.clients;

import static dev.responsive.kafka.internal.utils.Utils.extractThreadNameFromConsumerClientId;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistration;
import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Simple wrapper around the underlying ResponsiveConsumer that handles async stuff under ALOS.
 * <p>
 * The async consumer has two jobs: first, it registers and manages the lifecycle of the
 * async thread pool associated with this StreamThread. Second, it's used to make sure any
 * pending async records get flushed through the topology before committing offsets for
 * those records.
 * <p>
 * If EOS is enabled, the async consumer will not be used at all and the above responsibilities
 * fall instead to the {@link AsyncStreamsProducer}
 */
public class AsyncStreamsConsumer<K, V> extends DelegatingConsumer<K, V> {

  private final String streamThreadName;
  private final AsyncThreadPoolRegistry asyncThreadPoolRegistry;
  private final AsyncThreadPoolRegistration registration;

  public AsyncStreamsConsumer(
      final Consumer<K, V> delegate,
      final String clientId,
      final AsyncThreadPoolRegistry asyncThreadPoolRegistry
  ) {
    super(delegate);
    this.streamThreadName = extractThreadNameFromConsumerClientId(clientId);

    this.asyncThreadPoolRegistry = asyncThreadPoolRegistry;
    this.registration = asyncThreadPoolRegistry.startNewAsyncThreadPool(streamThreadName);
  }

  @Override
  public void close() {
    shutdownAsyncThreadPool();
    super.close();
  }

  @Override
  public void close(final Duration timeout) {
    shutdownAsyncThreadPool();
    super.close(timeout);
  }

  private void shutdownAsyncThreadPool() {
    if (!streamThreadName.equals(Thread.currentThread().getName())) {
      throw new IllegalStateException(String.format(
          "Attempted to close consumer for StreamThread %s from thread %s",
          streamThreadName, Thread.currentThread().getName())
      );
    }
    asyncThreadPoolRegistry.shutdownAsyncThreadPool(streamThreadName);
  }
}
