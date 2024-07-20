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

import static dev.responsive.kafka.internal.config.ConfigUtils.eosEnabled;

import dev.responsive.kafka.api.async.internals.AsyncThreadPoolRegistry;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;

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
