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

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class StoreQueryIntegrationTest {

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
    name = info.getDisplayName().replace("()", "");

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    createTopicsAndWait(admin, Map.of(inputTopic(), 1, outputTopic(), 1));
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
  public void testKeyValueStoreAllQuery() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (final ResponsiveKafkaStreams streams = buildKVStreams(properties, false)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      final List<KeyValueTimestamp<String, String>> records = Arrays.asList(
        new KeyValueTimestamp<>("A", "ignored", 0L),
        new KeyValueTimestamp<>("B", "ignored", 0L),
        new KeyValueTimestamp<>("C", "ignored", 0L),
        new KeyValueTimestamp<>("D", "ignored", 0L),
        new KeyValueTimestamp<>("E", "ignored", 0L)
      );

      // When:
      pipeRecords(producer, inputTopic(), records);

      // Then:
      properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
      final var kvs = readOutput(outputTopic(), 0, 5, true, properties);
      assertThat(
          kvs,
          hasItems(
              new KeyValue<>("A", 1),
              new KeyValue<>("B", 2),
              new KeyValue<>("C", 3),
              new KeyValue<>("D", 4),
              new KeyValue<>("E", 5))
      );
    }
  }

  @Test
  public void testKeyValueStoreRangeQuery() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (final ResponsiveKafkaStreams streams = buildKVStreams(properties, true)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      final List<KeyValueTimestamp<String, String>> records = Arrays.asList(
          new KeyValueTimestamp<>("A", "ignored", 0L),
          new KeyValueTimestamp<>("B", "ignored", 0L),
          new KeyValueTimestamp<>("C", "ignored", 0L),
          new KeyValueTimestamp<>("D", "ignored", 0L),
          new KeyValueTimestamp<>("E", "ignored", 0L)
      );

      // When:
      pipeRecords(producer, inputTopic(), records);

      // Then:
      properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
      final var kvs = readOutput(outputTopic(), 0, 5, true, properties);
      assertThat(
          kvs,
          hasItems(
              new KeyValue<>("A", 0),
              new KeyValue<>("B", 1),
              new KeyValue<>("C", 2),
              new KeyValue<>("D", 3),
              new KeyValue<>("E", 3))
      );
    }
  }

  private ResponsiveKafkaStreams buildKVStreams(
      final Map<String, Object> properties,
      final boolean range
  ) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> input = builder.stream(inputTopic());

    final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
        ResponsiveStores.keyValueStoreBuilder(
            ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.keyValue(kvStoreName())),
            Serdes.String(),
            Serdes.String());
    input
        .processValues(new TransformerSupplier(range, storeBuilder), kvStoreName())
        .to(outputTopic(), Produced.valueSerde(Serdes.Integer()));

    return new ResponsiveKafkaStreams(builder.build(), properties);
  }

  private class TransformerSupplier implements FixedKeyProcessorSupplier<String, String, Integer> {

    private final StoreBuilder<?> storeBuilder;
    private final boolean rangeQuery;

    public TransformerSupplier(final boolean rangeQuery, final StoreBuilder<?> storeBuilder) {
      this.rangeQuery = rangeQuery;
      this.storeBuilder = storeBuilder;
    }

    @Override
    public FixedKeyProcessor<String, String, Integer> get() {
      return new CountingProcessor(rangeQuery);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      if (storeBuilder != null) {
        return Collections.singleton(storeBuilder);
      }
      return null;
    }
  }

  private class CountingProcessor implements FixedKeyProcessor<String, String, Integer> {

    private final boolean rangeQuery;

    public CountingProcessor(final boolean rangeQuery) {
      this.rangeQuery = rangeQuery;
    }

    private KeyValueStore<String, String> kvStore;
    private FixedKeyProcessorContext<String, Integer> context;

    @Override
    public void init(final FixedKeyProcessorContext<String, Integer> context) {
      FixedKeyProcessor.super.init(context);
      this.kvStore = context.getStateStore(kvStoreName());
      this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<String, String> record) {
      kvStore.put(record.key(), record.value());
      int count = 0;
      if (rangeQuery) {
        try (final var iter = kvStore.range("B", "D")) {
          while (iter.hasNext()) {
            iter.next();
            ++count;
          }
        }
      } else {
        try (final var iter = kvStore.all()) {
          while (iter.hasNext()) {
            iter.next();
            ++count;
          }
        }
      }
      context.forward(record.withValue(count));
    }
  }

  private String kvStoreName() {
    return name + "-kv-store";
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}
