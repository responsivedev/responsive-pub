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

import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.SessionStore;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResponsiveSessionStoreIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INPUT_TOPIC = "input";
  private static final String OTHER_TOPIC = "other";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    // append a random int to avoid naming collisions on repeat runs
    name = info.getTestMethod().orElseThrow().getName() + "-" + new Random().nextInt();

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(otherTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  @Test
  public void shouldComputeSessionAggregate() throws Exception {
    // Given:
    final Duration inactivityGap = Duration.ofSeconds(5);
    final Duration gracePeriod = Duration.ofSeconds(2);
    final SessionWindows window =
        SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod);

    final Materialized<String, String, SessionStore<Bytes, byte[]>> store =
        ResponsiveStores.sessionMaterialized(
            ResponsiveSessionParams.session(name, inactivityGap, gracePeriod)
        );

    // Start from timestamp of 0L to get predictable results
    final List<KeyValueTimestamp<String, String>> inputEvents = asList(
        new KeyValueTimestamp<>("key", "a", 0L),
        new KeyValueTimestamp<>("key", "b", 1_000L),
        new KeyValueTimestamp<>("key", "c", 5_000L)
    );
    final List<KeyValue<Windowed<String>, String>> expectedPeeks = List.of(
        new KeyValue<>(windowedKey("key", 0, 0), "a"),
        new KeyValue<>(windowedKey("key", 0, 0), null),
        new KeyValue<>(windowedKey("key", 0, 1000), "ab"),
        new KeyValue<>(windowedKey("key", 0, 0), null),
        new KeyValue<>(windowedKey("key", 0, 1000), null),
        new KeyValue<>(windowedKey("key", 0, 5000), "abc")
    );
    final CountDownLatch outputLatch = new CountDownLatch(expectedPeeks.size());
    final List<KeyValue<Windowed<String>, String>> actualPeeks = new ArrayList<>();

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .windowedBy(window)
        .aggregate(() -> "", sessionAggregator(), sessionMerger(), store)
        .toStream()
        .peek((k, v) -> actualPeeks.add(new KeyValue<>(k, v)))
        .peek((k, v) -> outputLatch.countDown())
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    final Map<String, Object> properties = getMutablePropertiesWithStringSerdes();
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeRecords(producer, inputTopic(), inputEvents);

      assertThat(outputLatch.await(20_000, TimeUnit.MILLISECONDS), Matchers.equalTo(true));

      assertThat(actualPeeks, Matchers.hasSize(expectedPeeks.size()));
      for (var i = 0; i < actualPeeks.size(); i++) {
        var actual = actualPeeks.get(i);
        var expected = expectedPeeks.get(i);
        assertThat(actual.key, Matchers.equalTo(expected.key));
        assertThat(actual.value, Matchers.equalTo(expected.value));
      }
    }
  }

  @Test
  public void shouldComputeSessionAggregateWithMultipleKeys() throws Exception {
    // Given:
    final Duration inactivityGap = Duration.ofSeconds(5);
    final Duration gracePeriod = Duration.ofSeconds(2);
    final SessionWindows window =
        SessionWindows.ofInactivityGapAndGrace(inactivityGap, gracePeriod);

    final Materialized<String, String, SessionStore<Bytes, byte[]>> store =
        ResponsiveStores.sessionMaterialized(
            ResponsiveSessionParams.session(name, inactivityGap, gracePeriod)
        );

    // Start from timestamp of 0L to get predictable results
    final List<KeyValueTimestamp<String, String>> inputEvents = asList(
        new KeyValueTimestamp<>("key1", "a", 0L),
        new KeyValueTimestamp<>("key1", "b", 1_000L),
        new KeyValueTimestamp<>("key2", "c", 5_000L),
        new KeyValueTimestamp<>("key2", "d", 15_000L)
    );
    final List<KeyValue<Windowed<String>, String>> expectedPeeks = List.of(
        new KeyValue<>(windowedKey("key1", 0, 0), "a"),
        new KeyValue<>(windowedKey("key1", 0, 0), null),
        new KeyValue<>(windowedKey("key1", 0, 1000), "ab"),
        new KeyValue<>(windowedKey("key2", 5000, 5000), "c"),
        new KeyValue<>(windowedKey("key2", 15000, 15000), "d")
    );
    final CountDownLatch outputLatch = new CountDownLatch(expectedPeeks.size());
    final List<KeyValue<Windowed<String>, String>> actualPeeks = new ArrayList<>();

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .windowedBy(window)
        .aggregate(() -> "", sessionAggregator(), sessionMerger(), store)
        .toStream()
        .peek((k, v) -> actualPeeks.add(new KeyValue<>(k, v)))
        .peek((k, v) -> outputLatch.countDown())
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    final Map<String, Object> properties = getMutablePropertiesWithStringSerdes();
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeRecords(producer, inputTopic(), inputEvents);

      assertThat(outputLatch.await(20_000, TimeUnit.MILLISECONDS), Matchers.equalTo(true));

      assertThat(actualPeeks, Matchers.hasSize(expectedPeeks.size()));
      for (var i = 0; i < actualPeeks.size(); i++) {
        var actual = actualPeeks.get(i);
        var expected = expectedPeeks.get(i);
        assertThat(actual.key, Matchers.equalTo(expected.key));
        assertThat(actual.value, Matchers.equalTo(expected.value));
      }
    }
  }

  private Windowed<String> windowedKey(final String key, final long startMs, final long endMs) {
    return new Windowed<>(key, new SessionWindow(startMs, endMs));
  }

  private Map<String, Object> getMutablePropertiesWithStringSerdes() {
    final Map<String, Object> properties = getMutableProperties();
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    return properties;
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1); // commit as often as possible

    properties.put(consumerPrefix(REQUEST_TIMEOUT_MS_CONFIG), 5_000);
    properties.put(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 5_000 - 1);

    properties.put(consumerPrefix(MAX_POLL_RECORDS_CONFIG), 1);

    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);

    return properties;
  }

  public static Merger<String, String> sessionMerger() {
    return (aggKey, agg1, agg2) -> {
      if (agg1 == null) {
        return agg2;
      } else if (agg2 == null) {
        return agg1;
      }
      return agg1 + agg2;
    };
  }

  public static Aggregator<String, String, String> sessionAggregator() {
    return (k, v, agg) -> agg + v;
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  private String otherTopic() {
    return name + "." + OTHER_TOPIC;
  }
}
