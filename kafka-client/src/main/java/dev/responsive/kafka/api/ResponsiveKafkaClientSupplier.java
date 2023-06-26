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

package dev.responsive.kafka.api;

import dev.responsive.kafka.consumer.ResponsiveGlobalConsumer;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public class ResponsiveKafkaClientSupplier implements KafkaClientSupplier, Configurable {

  private final KafkaClientSupplier delegateClientSupplier = new DefaultKafkaClientSupplier();
  private Map<String, ?> config;

  @Override
  public void configure(final Map<String, ?> configs) {
    config = configs;
  }

  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    return delegateClientSupplier.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
    return delegateClientSupplier.getProducer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
    return delegateClientSupplier.getConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
    return delegateClientSupplier.getRestoreConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
    if (!this.config.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      throw new IllegalArgumentException(
          "Expected configuration from ResponsiveKafkaClientSupplier to include application.id");
    }

    final String groupId = this.config.get(StreamsConfig.APPLICATION_ID_CONFIG) + "-global";
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

    return new ResponsiveGlobalConsumer(
        config,
        delegateClientSupplier.getGlobalConsumer(config),
        getAdmin(config)
    );
  }
}
