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

package dev.responsive.spring.config;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.util.Properties;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.kafka.config.KafkaStreamsCustomizer;

public interface ResponsiveKafkaStreamsCustomizer extends KafkaStreamsCustomizer {
  @Override
  default KafkaStreams initKafkaStreams(
      Topology topology,
      Properties properties,
      KafkaClientSupplier clientSupplier
  ) {
    return new ResponsiveKafkaStreams(topology, properties, clientSupplier);
  }
}
