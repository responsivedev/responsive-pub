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

package dev.responsive.kafka.internal.clients;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaClientSupplier;

public class AsyncStreamsKafkaClientSupplier implements KafkaClientSupplier {

  private final KafkaClientSupplier delegateKafkaClientSupplier;
  private final AsyncThreadPoolRegistry asyncThreadPoolRegistry;

  public AsyncStreamsKafkaClientSupplier(
      final KafkaClientSupplier delegateKafkaClientSupplier,
      final AsyncThreadPoolRegistry asyncThreadPoolRegistry
  ) {
    this.delegateKafkaClientSupplier = delegateKafkaClientSupplier;
    this.asyncThreadPoolRegistry = asyncThreadPoolRegistry;
  }

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return delegateKafkaClientSupplier.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    final var delegateProducer = delegateKafkaClientSupplier.getProducer(config);

    return new AsyncStreamsProducer<>(
        delegateProducer,
        (String) config.get(ProducerConfig.CLIENT_ID_CONFIG),
        asyncThreadPoolRegistry
    );
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    final var delegateConsumer = delegateKafkaClientSupplier.getConsumer(config);

    return new AsyncStreamsConsumer<>(
        delegateConsumer,
        (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG),
        asyncThreadPoolRegistry
    );
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return delegateKafkaClientSupplier.getRestoreConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    return delegateKafkaClientSupplier.getGlobalConsumer(config);
  }

}
