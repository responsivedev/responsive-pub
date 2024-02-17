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

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this test can be run manually to verify Cluster Bootstrapping behavior
@Disabled
class ChangelogMigrationToolTest {

  private static final Logger LOG = LoggerFactory.getLogger(ChangelogMigrationToolTest.class);

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
    // Given:
    final int partitions = 2;
    final int numKeys = 100;
    final long numEvents = 1000;

    final var tableName = name + "-count";
    final var changelog = name + "-count-changelog";
    final var params = ResponsiveKeyValueParams.keyValue(tableName);

    final Map<String, Object> baseProps = getProperties();
    final Properties bootProps = new Properties();
    bootProps.putAll(getProperties());
    bootProps.put(ChangelogMigrationConfig.CHANGELOG_TOPIC_CONFIG, changelog);

    // When:
    // 1: Run A Normal KS Application with rocksDB and make sure all records are processed
    LOG.info("Running a normal Kafka Streams application with RocksDB to populate changelog.");
    final KafkaProducer<Long, Long> produce = new KafkaProducer<>(baseProps);
    try (final ResponsiveKafkaStreams streams = buildRocksBased(baseProps)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      final long[] keys = LongStream.range(0, numKeys).toArray();
      final long perKey = numEvents / numKeys;
      pipeInput(inputTopic(), partitions, produce, System::currentTimeMillis, 0, perKey, keys);

      LOG.info("Awaiting the output from all 1000 records");
      readOutput(outputTopic(), 0, numEvents, true, baseProps);
    }

    // 2: Run the changelog migration tool to bootstrap the new Cassandra table
    final CountDownLatch processedAllRecords = new CountDownLatch(1000);
    final Consumer<Record<byte[], byte[]>> countdown = r -> processedAllRecords.countDown();
    final ChangelogMigrationTool app = new ChangelogMigrationTool(bootProps, params, countdown);

    LOG.info("Awaiting for Bootstrapping application to start");
    startAppAndAwaitRunning(Duration.ofSeconds(120), app.getStreams());

    // Then:
    // Ensure that all records exist with the expected values
    // before checking Cassandra make sure all records have been processed
    LOG.info("Await all records from changelog processed");
    processedAllRecords.await();

    final ReadOnlyKeyValueStore<byte[], byte[]> table = app.getStreams()
        .store(StoreQueryParameters.fromNameAndType(
            tableName, QueryableStoreTypes.keyValueStore()));

    final Map<Long, Long> all = new HashMap<>();
    try (final var ser = new LongDeserializer(); final var it = table.all()) {
      while (it.hasNext()) {
        final var kv = it.next();
        all.put(ser.deserialize("ignored", kv.key), ser.deserialize("ignored", kv.value));
      }
    }

    for (long k = 0; k < numKeys; k++) {
      assertThat(all, Matchers.hasEntry(k, numEvents/ numKeys));
    }

    // Finally: Cleanup
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