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

package dev.responsive.spring.annotations;

import dev.responsive.spring.config.ResponsiveKafkaStreamsCustomizer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration(proxyBeanMethods = false)
public class ResponsiveDefaultConfiguration extends KafkaStreamsDefaultConfiguration {

  @Bean(name = DEFAULT_STREAMS_BUILDER_BEAN_NAME)
  @Override
  public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder(
      @Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
      final ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider,
      final ObjectProvider<StreamsBuilderFactoryBeanConfigurer> configurerProvider
  ) {
    KafkaStreamsConfiguration streamsConfig = streamsConfigProvider.getIfAvailable();
    if (streamsConfig != null) {
      StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean(streamsConfig);
      configurerProvider.orderedStream().forEach(configurer -> configurer.configure(fb));
      fb.setKafkaStreamsCustomizer((ResponsiveKafkaStreamsCustomizer) kafkaStreams -> { });
      return fb;
    } else {
      throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
          DEFAULT_STREAMS_BUILDER_BEAN_NAME, "streamsConfig", "There is no '"
          + DEFAULT_STREAMS_CONFIG_BEAN_NAME + "' " + KafkaStreamsConfiguration.class.getName()
          + " bean in the application context.\n"
          + "Consider declaring one or don't use @EnableKafkaStreams."
      );
    }
  }
}
