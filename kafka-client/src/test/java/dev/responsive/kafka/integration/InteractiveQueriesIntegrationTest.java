/*
 * Copyright 2024 Responsive Computing, Inc.
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
import static dev.responsive.kafka.testutils.IntegrationTestUtils.getStore;
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
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.timestampedKeyValueStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class InteractiveQueriesIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

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
  public void shouldReadLatestValueAndTimestampFromKVStoreWithIQv1() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (final ResponsiveKafkaStreams streams = buildKVStoreApp(properties)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), streams);

      final List<KeyValueTimestamp<String, String>> input = Arrays.asList(
          new KeyValueTimestamp<>("A", "a", 1L),
          new KeyValueTimestamp<>("B", "b", 2L),
          new KeyValueTimestamp<>("C", "c", 3L)
      );

      // When:
      pipeRecords(producer, inputTopic(), input);

      // await output to make sure we've processed all the input so far
      long readOffset = 0L;
      int outputCount = 3;
      final var output = readOutput(outputTopic(), readOffset, outputCount, true, properties);
      assertThat(output.size(), equalTo(outputCount));

      final var store = getStore(streams, kvStoreName(), timestampedKeyValueStore());

      // Then:
      assertThat(store.get("A"), equalTo(ValueAndTimestamp.make("a", 1L)));
      assertThat(store.get("B"), equalTo(ValueAndTimestamp.make("b", 2L)));
      assertThat(store.get("C"), equalTo(ValueAndTimestamp.make("c", 3L)));

      // And Given:
      final List<KeyValueTimestamp<String, String>> input2 = Arrays.asList(
          new KeyValueTimestamp<>("A", "x", 4L),
          new KeyValueTimestamp<>("B", "y", 5L),
          new KeyValueTimestamp<>("C", "z", 6L)
      );

      // When:
      pipeRecords(producer, inputTopic(), input2);

      readOffset = 3L;
      final var output2 = readOutput(outputTopic(), readOffset, outputCount, true, properties);
      assertThat(output2.size(), equalTo(outputCount));

      // Then:
      assertThat(store.get("A"), equalTo(ValueAndTimestamp.make("ax", 4L)));
      assertThat(store.get("B"), equalTo(ValueAndTimestamp.make("by", 5L)));
      assertThat(store.get("C"), equalTo(ValueAndTimestamp.make("cz", 6L)));
    }
  }

  private ResponsiveKafkaStreams buildKVStoreApp(final Map<String, Object> properties) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> input = builder.stream(inputTopic());
    final StoreBuilder<TimestampedKeyValueStore<String, String>> storeBuilder =
        ResponsiveStores.timestampedKeyValueStoreBuilder(
            ResponsiveStores.keyValueStore(ResponsiveKeyValueParams.keyValue(kvStoreName())),
            Serdes.String(),
            Serdes.String());
    input
        .process(new MyProcessorSupplier(storeBuilder), kvStoreName())
        .to(outputTopic());

    return new ResponsiveKafkaStreams(builder.build(), properties);
  }

  private static class TimestampAppendingProcessor
      implements Processor<String, String, String, String> {

    private final String storeName;

    private ProcessorContext<String, String> context;
    private TimestampedKeyValueStore<String, String> store;

    public TimestampAppendingProcessor(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(final ProcessorContext<String, String> context) {
      Processor.super.init(context);
      this.context = context;
      this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(final Record<String, String> record) {
      final ValueAndTimestamp<String> oldValue = store.get(record.key());
      final String newValue = oldValue == null
          ? record.value()
          : oldValue.value() + record.value();

      store.put(record.key(), ValueAndTimestamp.make(newValue, record.timestamp()));
      context.forward(new Record<>(record.key(), newValue, record.timestamp()));
    }
  }

  private static class MyProcessorSupplier
      implements ProcessorSupplier<String, String, String, String> {

    private final StoreBuilder<?> storeBuilder;

    public MyProcessorSupplier(final StoreBuilder<?> storeBuilder) {
      this.storeBuilder = storeBuilder;
    }

    @Override
    public Processor<String, String, String, String> get() {
      return new TimestampAppendingProcessor(storeBuilder.name());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      return Collections.singleton(storeBuilder);
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
