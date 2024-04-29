/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.async;

import static dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier;
import dev.responsive.kafka.testutils.SimpleStatefulProcessorSupplier.SimpleProcessorOutput;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class AsyncProcessorIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private static final String IN_KV_STORE = "in-kv-store";
  private static final String ASYNC_KV_STORE = "async-kv-store";
  private static final String OUT_KV_STORE = "out-kv-store";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  private final int numEventsPerKey = 5;

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
    createTopicsAndWait(admin, Map.of(inputTopic(), 10, outputTopic(), 1));
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
  public void shouldProcessEventsInOrderByKey() throws Exception {
    // Given:
    final List<String> keys = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                                      "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                                      "w", "x", "y", "z");

    // produce 5 records for each key, with keys interleaved across iterations
    final List<KeyValue<String, String>> inputRecords = new LinkedList<>();
    for (int val = 1; val < 1 + numEventsPerKey; ++val) {
      for (final String key : keys) {
        inputRecords.add(new KeyValue<>(key, key + val));
      }
    }

    final Map<String, String> finalOutputRecords = new HashMap<>(keys.size());
    for (final String key : keys) {
      final StringBuilder finalValueStringBuilder = new StringBuilder();

      for (int val = 1; val < 1 + numEventsPerKey; ++val) {
        finalValueStringBuilder.append(String.format("%s:IN:%d--", key, val));
      }

      finalValueStringBuilder.append(String.format("%s:END:%d", key, numEventsPerKey));

      finalOutputRecords.put(key, finalValueStringBuilder.toString());
    }

    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    
    final AtomicInteger processed = new AtomicInteger(0);
    final Map<String, String> latestValues = new ConcurrentHashMap<>();

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .processValues(
            new SimpleStatefulProcessorSupplier(
                this::computeNewValueForSourceProcessor,
                ResponsiveKeyValueParams.fact(IN_KV_STORE)),
            IN_KV_STORE)
        .processValues(
            createAsyncProcessorSupplier(
                new SimpleStatefulProcessorSupplier(
                    this::computeNewValueForAsyncProcessor,
                    ResponsiveKeyValueParams.fact(ASYNC_KV_STORE),
                    processed
                )),
            Named.as("AsyncProcessor"),
            ASYNC_KV_STORE)
        .processValues(
            new SimpleStatefulProcessorSupplier(
                this::computeNewValueForSinkProcessor,
                ResponsiveKeyValueParams.fact(OUT_KV_STORE),
                latestValues),
            OUT_KV_STORE)
        .to(outputTopic());

    // The total number of records processed, equal to the total number of output records
    // ONLY when caching is disabled
    final int numProcessedRecords = keys.size() * numEventsPerKey;

    try (final var streams = new ResponsiveKafkaStreams(builder.build(), properties)) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      // When:
      pipeRecords(producer, inputTopic(), inputRecords);

      // Then:
      final var kvs = readOutput(outputTopic(), 0, numProcessedRecords, true, properties);

      for (final String key : keys) {
        final String finalValue = finalOutputRecords.get(key);
        assertThat(kvs, hasItem(new KeyValue<>(key, finalValue)));
      }
    }
    assertThat(processed.get(), equalTo(numProcessedRecords));
    assertThat(latestValues, equalTo(finalOutputRecords));
  }

  // The "in"" processor is a simple counter that just forwards the new count appended to
  // the key and processor name ("IN")
  // The IN count should always match the END count computed downstream
  private SimpleProcessorOutput computeNewValueForSourceProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, String> inputRecord
  ) {
    final int newCount = oldValAndTimestamp == null
        ? 1
        : 1 + Integer.parseInt(oldValAndTimestamp.value());

    final String forwardedVal = String.format("%s:%s:%d", inputRecord.key(), "IN", newCount);
    return new SimpleProcessorOutput(forwardedVal, Integer.toString(newCount));
  }

  // The async processor always forwards the same value that it computes, so over time
  // it appends all records together into one long chain.
  // The values computed and saved/forwarded here will include the upstream IN processor results
  // as a prefix, but won't include the END processor suffix in the async processor results
  // since by definition, that won't be added until the downstream END processor appends
  // its own suffix
  private SimpleProcessorOutput computeNewValueForAsyncProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, String> inputRecord
  ) {
    if (oldValAndTimestamp == null) {
      return new SimpleProcessorOutput(inputRecord.value());
    }

    final String newVal = String.format("%s--%s", oldValAndTimestamp.value(), inputRecord.value());
    return new SimpleProcessorOutput(newVal);
  }

  // The "end"" processor is a slightly-more-advanced counter that just forwards the new count
  // appended to the key and processor name ("END") as well as the input record value
  // The END count should always match the IN count computed upstream
  private SimpleProcessorOutput computeNewValueForSinkProcessor(
      final ValueAndTimestamp<String> oldValAndTimestamp,
      final FixedKeyRecord<String, String> inputRecord
  ) {
    final int newCount = oldValAndTimestamp == null
        ? 1
        : 1 + Integer.parseInt(oldValAndTimestamp.value());

    final String forwardedVal = String.format(
        "%s--%s:%s:%d", inputRecord.value(), inputRecord.key(), "END", newCount
    );
    return new SimpleProcessorOutput(forwardedVal, Integer.toString(newCount));
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);
    
    properties.put(ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, 5);
    properties.put(NUM_STREAM_THREADS_CONFIG, 5);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}
