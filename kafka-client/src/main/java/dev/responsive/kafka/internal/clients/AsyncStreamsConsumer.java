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

package dev.responsive.kafka.internal.clients;

import static dev.responsive.kafka.internal.utils.Utils.extractThreadNameFromConsumerClientId;

import dev.responsive.kafka.internal.async.AsyncThreadPoolRegistration;
import dev.responsive.kafka.internal.async.AsyncThreadPoolRegistry;
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
