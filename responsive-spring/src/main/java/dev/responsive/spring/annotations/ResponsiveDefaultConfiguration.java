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

package dev.responsive.spring.annotations;

import dev.responsive.spring.config.ResponsiveFactoryBean;
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
      StreamsBuilderFactoryBean fb = new ResponsiveFactoryBean(streamsConfig);
      configurerProvider.orderedStream().forEach(configurer -> configurer.configure(fb));
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
