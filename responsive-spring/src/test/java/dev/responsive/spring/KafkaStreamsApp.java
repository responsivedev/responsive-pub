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

package dev.responsive.spring;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.spring.annotations.EnableResponsive;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Configuration
@EnableKafka
@EnableResponsive
public class KafkaStreamsApp {

  public static void main(String[] args) throws NoSuchMethodException {
    SpringApplication.run(KafkaStreamsApp.class, args);
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration streamsConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG, StorageBackend.MONGO_DB.name());
    props.put(ResponsiveConfig.MONGO_ENDPOINT_CONFIG, "mongodb://localhost:27017");
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        RecoveringDeserializationExceptionHandler.class);
    props.put(
        RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
        recoverer()
    );
    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public StreamsBuilderFactoryBeanConfigurer moreMagic() {
    return fb -> fb.setStateListener((newState, oldState) -> {
      System.out.println("State transition from " + oldState + " to " + newState);
    });
  }

  @Bean
  public KafkaAdmin admin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return new KafkaAdmin(configs);
  }

  @Bean
  public NewTopic topic1() {
    return TopicBuilder.name("streamingTopic1")
        .build();
  }

  @Bean
  public KStream<String, String> magic(StreamsBuilder streamBuilder, KafkaAdmin admin) {
    final Map<String, TopicDescription> streamingTopic1 = admin.describeTopics("streamingTopic1");
    System.out.println(streamingTopic1);

    KStream<String, String> stream = streamBuilder.stream("streamingTopic1");
    stream
        .mapValues((ValueMapper<String, String>) String::toUpperCase)
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(1_000)))
        .reduce((String value1, String value2) -> value1 + value2,
            Named.as("windowStore"))
        .toStream()
        .map((windowedId, value) -> new KeyValue<>(windowedId.key(), value))
        .filter((i, s) -> s.length() > 40)
        .to("streamingTopic2");

    stream.print(Printed.toSysOut());
    return stream;
  }

  @Bean
  public DeadLetterPublishingRecoverer recoverer() {
    return new DeadLetterPublishingRecoverer(kafkaTemplate(),
        (record, ex) -> new TopicPartition("recovererDLQ", -1));
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092");
    configProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class);
    configProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

}