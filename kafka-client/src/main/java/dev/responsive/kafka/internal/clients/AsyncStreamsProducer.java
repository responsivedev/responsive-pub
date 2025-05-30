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

import static dev.responsive.kafka.internal.utils.Utils.extractThreadNameFromProducerClientId;

import dev.responsive.kafka.internal.async.AsyncThreadPoolRegistry;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * Simple wrapper around the underlying ResponsiveProducer that handles async stuff under EOS.
 * <p>
 * The async producer has two jobs: first, it registers and manages the lifecycle of the
 * async thread pool associated with this StreamThread. Second, it's used to make sure any
 * pending async records get flushed through the topology before committing a transaction with
 * those records.
 * <p>
 * If ALOS is used, the async producer will not be used at all and the above responsibilities
 * fall instead to the {@link AsyncStreamsConsumer}
 */
public class AsyncStreamsProducer<K, V> extends DelegatingProducer<K, V> {

  private final String streamThreadName;
  private final Runnable flushAsyncProcessors;

  public AsyncStreamsProducer(
      final Producer<K, V> delegate,
      final String clientId,
      final AsyncThreadPoolRegistry asyncThreadPoolRegistry
  ) {
    super(delegate);
    this.streamThreadName = extractThreadNameFromProducerClientId(clientId);

    final var asyncThreadPoolRegistration = asyncThreadPoolRegistry
        .startNewAsyncThreadPool(streamThreadName);
    this.flushAsyncProcessors = asyncThreadPoolRegistration::flushAllAsyncEvents;
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    // TODO: we should check for unflushed records here and throw an exception
    //  once EOSv1 is officially deprecated, before that it's possible that some
    //  tasks are not part of this commit and in theory could still be processing
    super.commitTransaction();
  }
  
  @Override
  public void flush() {
    flushAsyncProcessors.run();
    super.flush();
  }
}
