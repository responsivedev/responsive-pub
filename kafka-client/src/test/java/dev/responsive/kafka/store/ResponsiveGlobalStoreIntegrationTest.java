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

package dev.responsive.kafka.store;

import static dev.responsive.utils.IntegrationTestUtils.pipeInput;
import static dev.responsive.utils.IntegrationTestUtils.readOutput;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.ResponsiveStores;
import dev.responsive.utils.ContainerExtension;
import dev.responsive.utils.IntegrationTestUtils;
import dev.responsive.utils.ResponsiveConfigParam;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ContainerExtension.class)
public class ResponsiveGlobalStoreIntegrationTest {

  private static final Logger LOG
      = LoggerFactory.getLogger(ResponsivePartitionedStoreEosIntegrationTest.class);

  private static final int MAX_POLL_MS = 5000;
  private static final String INPUT_TOPIC = "input";
  private static final String GLOBAL_TOPIC = "global";
  private static final String OUTPUT_TOPIC = "output";
  private static final String STORE_NAME = "global-store";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    name = info.getTestMethod().orElseThrow().getName();
    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    admin.createTopics(
        List.of(
            new NewTopic(INPUT_TOPIC, Optional.of(2), Optional.empty()),
            new NewTopic(GLOBAL_TOPIC, Optional.of(2), Optional.empty()),
            new NewTopic(OUTPUT_TOPIC, Optional.of(1), Optional.empty())
        )
    );
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(INPUT_TOPIC, GLOBAL_TOPIC, OUTPUT_TOPIC));
  }

  @Test
  public void shouldUseGlobalTable() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final var streams = buildStreams(properties)
    ) {
      // When:
      pipeInput(GLOBAL_TOPIC, 2, producer, System::currentTimeMillis, 0, 3, 0, 1);
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      pipeInput(INPUT_TOPIC, 2, producer, System::currentTimeMillis, 0, 10, 0, 1);

      // Then:
      final Set<KeyValue<Long, Long>> result = new HashSet<>(
          readOutput(OUTPUT_TOPIC, 0, 20, false, properties));

      for (int k = 0; k < 2; k++) {
        for (int i = 0; i < 10; i++) {
          MatcherAssert.assertThat(result, Matchers.hasItem(new KeyValue<>((long) k, i + 2L)));
        }
      }
    }
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, LongSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);

    // this ensures we can control the commits by explicitly requesting a commit
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 100);

    return properties;
  }

  private ResponsiveKafkaStreams buildStreams(
      final Map<String, Object> properties
  ) {
    final StreamsBuilder builder = new StreamsBuilder();

    final GlobalKTable<Long, Long> globalTable = builder.globalTable(
        GLOBAL_TOPIC,
        ResponsiveStores.materialized(STORE_NAME)
    );

    final KStream<Long, Long> stream = builder.stream(INPUT_TOPIC);
    stream.join(globalTable, (k, v) -> k, Long::sum)
        .to(OUTPUT_TOPIC);

    return ResponsiveKafkaStreams.create(builder.build(), properties);
  }

}
