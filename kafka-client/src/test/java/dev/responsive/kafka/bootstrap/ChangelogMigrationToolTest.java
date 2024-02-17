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

package dev.responsive.kafka.bootstrap;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeInput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.OPTIMIZE;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

// this test can be run manually to verify Cluster Bootstrapping behavior
@Disabled
class ChangelogMigrationToolTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    // add displayName to name to account for parameterized tests
    final Random random = new Random();
    name = info.getDisplayName().replace("()", "") + random.nextInt();
    ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    createTopicsAndWait(admin, Map.of(inputTopic(), 2, outputTopic(), 1));
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), outputTopic()));
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  @Test
  public void test() throws Exception {
    final Map<String, Object> properties = getProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    // Run A Normal KS Application with rocksDB
    try (final ResponsiveKafkaStreams streams = buildRocksBased(properties)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      for (int i = 0; i < 1000; i++) {
        pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 0, 10, 0, 1);
      }

      // just verify all 1000 were written
      readOutput(outputTopic(), 0, 1000, true, properties);
    }

    // Run the bootstrap application with Cassandra
    final Properties bootstrapProps = new Properties();
    bootstrapProps.putAll(getProperties());
    bootstrapProps.put(ChangelogMigrationConfig.CHANGELOG_TOPIC_CONFIG, name + "-count-changelog");
    bootstrapProps.put(ChangelogMigrationConfig.TABLE_NAME_CONFIG, name + "-count");
    final ChangelogMigrationTool app =
        new ChangelogMigrationTool(bootstrapProps);
    startAppAndAwaitRunning(Duration.ofSeconds(120), app.getStreams());

    // Manually Check Cassandra
    Thread.sleep(100000);

    app.getStreams().close();
    app.getStreams().cleanUp();
  }

  private ResponsiveKafkaStreams buildRocksBased(final Map<String, Object> properties) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .count(Materialized.as(Stores.persistentKeyValueStore("count")))
        .toStream()
        .to(outputTopic());

    return new ResponsiveKafkaStreams(builder.build(), properties);
  }

  private Map<String, Object> getProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1);
    properties.put(TOPOLOGY_OPTIMIZATION_CONFIG, OPTIMIZE);
    properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}