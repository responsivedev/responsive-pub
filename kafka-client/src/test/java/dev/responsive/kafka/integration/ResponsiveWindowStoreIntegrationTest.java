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

import static dev.responsive.kafka.testutils.IntegrationTestUtils.minutesToMillis;
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
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResponsiveWindowStoreIntegrationTest {

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
    this.responsiveProps.put(ResponsiveConfig.WINDOW_BLOOM_FILTER_COUNT_CONFIG, 1);
    this.responsiveProps.put(ResponsiveConfig.WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG, 10);

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

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), otherTopic(), outputTopic()));
  }

  @Test
  public void shouldComputeTumblingWindowAggregateWithRetention() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<Long>, Long> collect = new ConcurrentHashMap<>();
    final CountdownLatchWrapper outputLatch = new CountdownLatchWrapper(0);

    final CountDownLatch finalLatch = new CountDownLatch(2);
    final KStream<Long, Long> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
        .aggregate(
            () -> 0L,
            (k, v, agg) -> agg + v,
            ResponsiveStores.windowMaterialized(
                ResponsiveWindowParams.window(
                    name,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(1)
                )
            )
        )
        .toStream()
        .peek((k, v) -> {
          collect.put(k, v);
          outputLatch.countDown();

          if (v == 1000) {
            finalLatch.countDown();
          }
        })
        // discard the window, so we don't have to serialize it
        // we're not checking the output topic anyway
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = (System.currentTimeMillis() / 60_000) * 60_000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      // produce two keys each having values 0-2 that are 100ms apart
      // which should result in just one window (per key) with the sum
      // of 3 after the first Streams instance is closed
      outputLatch.resetCountdown(6);
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 0, 3, 0, 1);
      outputLatch.await();

      assertThat(collect, Matchers.hasEntry(windowed(0, baseTs, 5000), 3L));
    }

    // force a commit/flush so that we can test Cassandra by closing
    // the old Kafka Streams and creating a new one
    properties.put(APPLICATION_SERVER_CONFIG, "host2:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams = new ResponsiveKafkaStreams(
            builder.build(),
            properties
        );
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      outputLatch.resetCountdown(4);
      // Produce another two records with values 3-4, still within the first window
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 3, 5, 0, 1);

      outputLatch.await();
      assertThat(collect, Matchers.hasEntry(windowed(0, baseTs, 5000), 10L));

      // Produce another set with values 5-10 in new window that advances stream-time
      // enough to expire the first window
      outputLatch.resetCountdown(10);
      final long secondWindowStart = baseTs + 10_000L;
      timestamp.set(secondWindowStart);
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 5, 10, 0, 1);

      outputLatch.await();
      assertThat(collect, Matchers.hasEntry(windowed(0, secondWindowStart, 5000), 35L));

      // at this point the records produced by this pipe should be
      // past the retention period and therefore be ignored from the
      // first window
      outputLatch.resetCountdown(2);
      timestamp.set(baseTs);
      pipeInput(producer, inputTopic(), timestamp::get, 100, 101, 0, 1);

      outputLatch.await();
      assertThat(collect, Matchers.hasEntry(windowed(0, baseTs, 5000), 10L));

      // use this to signify that we're done processing and count
      // down the latch
      outputLatch.resetCountdown(2);
      timestamp.set(baseTs + 15000);

      pipeInput(producer, inputTopic(), timestamp::get, 1000, 1001, 0, 1);
      finalLatch.await();
    }

    outputLatch.await();

    // Then:
    assertThat(collect.size(), Matchers.is(6));

    assertThat(collect, Matchers.hasEntry(windowed(0, baseTs, 5000), 10L));
    assertThat(collect, Matchers.hasEntry(windowed(1, baseTs, 5000), 10L));

    assertThat(collect, Matchers.hasEntry(windowed(0, baseTs + 10_000, 5000), 35L));
    assertThat(collect, Matchers.hasEntry(windowed(1, baseTs + 10_000, 5000), 35L));

    assertThat(collect, Matchers.hasEntry(windowed(0, baseTs + 15_000, 5000), 1000L));
    assertThat(collect, Matchers.hasEntry(windowed(1, baseTs + 15_000, 5000), 1000L));
  }

  @Test
  public void shouldComputeMultipleWindowsPerSegment() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutablePropertiesWithStringSerdes();
    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<String>, String> results = new ConcurrentHashMap<>();
    final CountdownLatchWrapper outputLatch = new CountdownLatchWrapper(0);

    final KStream<String, String> input = builder.stream(inputTopic());

    final Duration windowSize = Duration.ofMinutes(15);
    final Duration gracePeriod = Duration.ofDays(15);
    final long numSegments = 35L;

    input
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod))
        .aggregate(
            () -> "",
            (k, v, agg) -> agg + v,
            ResponsiveStores.windowMaterialized(
                ResponsiveWindowParams.window(
                    name,
                    windowSize,
                    gracePeriod
                ).withNumSegments(numSegments)
            )
        )
        .toStream()
        .peek((k, v) -> {
          results.put(k, v);
          outputLatch.countDown();
        })
        // discard the window, so we don't have to serialize it
        // we're not checking the output topic anyway
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      outputLatch.resetCountdown(11);

      // Start from timestamp of 0L to get predictable results
      // Write to multiple windows within a single segment and all within the large grace period
      final List<KeyValueTimestamp<String, String>> input1 = asList(
          new KeyValueTimestamp<>("key", "a", minutesToMillis(0L)),  // [0, 15]  --> "a"
          new KeyValueTimestamp<>("key", "b", minutesToMillis(11L)), // [0, 15]  --> "ab"
          new KeyValueTimestamp<>("key", "c", minutesToMillis(7L)),  // [0, 15]  --> "abc"
          new KeyValueTimestamp<>("key", "d", minutesToMillis(16L)), // [15, 30] --> "d"
          new KeyValueTimestamp<>("key", "e", minutesToMillis(28L)), // [15, 30] --> "de"
          new KeyValueTimestamp<>("key", "f", minutesToMillis(5L)),  // [0, 15]  --> "abcf"
          new KeyValueTimestamp<>("key", "g", minutesToMillis(26L)), // [15, 30] --> "deg"
          new KeyValueTimestamp<>("key", "h", minutesToMillis(41L)), // [30, 45] --> "h"
          new KeyValueTimestamp<>("key", "i", minutesToMillis(31L)), // [30, 45] --> "hi"
          new KeyValueTimestamp<>("key", "j", minutesToMillis(2L)),  // [0, 15]  --> "abcfj"
          new KeyValueTimestamp<>("key", "k", minutesToMillis(16L))  // [15, 30] --> "degk"
      );

      pipeRecords(producer, inputTopic(), input1);

      outputLatch.await();
      assertThat(results.size(), equalTo(3));
      assertThat(results, Matchers.hasEntry(windowedKeyInMinutes(0L, 15L), "abcfj"));
      assertThat(results, Matchers.hasEntry(windowedKeyInMinutes(15L, 30L), "degk"));
      assertThat(results, Matchers.hasEntry(windowedKeyInMinutes(30L, 45L), "hi"));
    }
  }

  @Test
  public void shouldComputeHoppingWindowAggregateWithRetention() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutablePropertiesWithStringSerdes();

    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<String>, String> results = new ConcurrentHashMap<>();
    final CountdownLatchWrapper outputLatch = new CountdownLatchWrapper(0);

    final Duration windowSize = Duration.ofSeconds(10);
    final Duration gracePeriod = Duration.ofSeconds(5);
    final Duration advance = Duration.ofSeconds(5);

    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advance))
        .aggregate(
            () -> "",
            (k, v, agg) -> agg + v,
            ResponsiveStores.windowMaterialized(
                ResponsiveWindowParams.window(
                    name,
                    windowSize,
                    gracePeriod
                )
            )
        )
        .toStream()
        .peek((k, v) -> {
          results.put(k, v);
          outputLatch.countDown();
        })
        // discard the window, so we don't have to serialize it
        // we're not checking the output topic anyway
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    // When:
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      outputLatch.resetCountdown(11);

      // Start from timestamp of 0L to get predictable results
      final List<KeyValueTimestamp<String, String>> input1 = asList(
          new KeyValueTimestamp<>("key", "a", 0L),
          new KeyValueTimestamp<>("key", "b", 6_000L),
          new KeyValueTimestamp<>("key", "c", 8_000L),
          new KeyValueTimestamp<>("key", "d", 16_000L), // windowCloseTime = 11s --> [0, 10] closed
          new KeyValueTimestamp<>("key", "e", 8_000L),  // within grace for [5, 15s] window
          new KeyValueTimestamp<>("key", "f", 0L),     // outside grace for all windows
          new KeyValueTimestamp<>("key", "g", 11_000L),
          new KeyValueTimestamp<>("key", "h", 5_000L)   // within grace for [5, 15s] window
      );

      pipeRecords(producer, inputTopic(), input1);
      outputLatch.await();

      assertThat(results.size(), equalTo(4));
      assertThat(results, Matchers.hasEntry(windowedKey(0L), "abc"));    // [0, 10s]
      assertThat(results, Matchers.hasEntry(windowedKey(5000L), "bcegh")); // [5, 15s]
      assertThat(results, Matchers.hasEntry(windowedKey(10000L), "dg"));  // [10s, 20s]
      assertThat(results, Matchers.hasEntry(windowedKey(15000L), "d"));   // [15s, 25s]
    }
  }

  static class CountdownLatchWrapper {
    private CountDownLatch currentLatch;

    public CountdownLatchWrapper(final int initialCountdown) {
      currentLatch = new CountDownLatch(initialCountdown);
    }

    public void countDown() {
      currentLatch.countDown();
    }

    public void resetCountdown(final int countdown) {
      currentLatch = new CountDownLatch(countdown);
    }

    public boolean await() {
      try {
        return currentLatch.await(60, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
    }
  }

  //@Test
  public void shouldComputeWindowedJoinUsingRanges() throws InterruptedException {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(inputTopic());
    final KStream<Long, Long> other = builder.stream(otherTopic());
    final Map<Long, Queue<Long>> collect = new ConcurrentHashMap<>();

    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(2);
    final Duration windowSize = Duration.ofMillis(1000);
    input.join(
            other,
            (v1, v2) -> {
              System.out.println("Joining: " + v1 + ", " + v2);
              return (v1 << Integer.SIZE) | v2;
            },
            JoinWindows.ofTimeDifferenceWithNoGrace(windowSize.dividedBy(2)),
            StreamJoined.with(
                ResponsiveStores.windowStoreSupplier(
                    "input" + name, windowSize, windowSize, true),
                ResponsiveStores.windowStoreSupplier(
                    "other" + name, windowSize, windowSize, true)
            )
        )
        .peek((k, v) -> {
          collect.computeIfAbsent(k, old -> new ArrayBlockingQueue<>(10)).add(v);
          if (v.intValue() == 1) {
            latch1.countDown();
          }
          if (v.intValue() == 100) {
            latch2.countDown();
          }
        })
        .to(outputTopic());

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = System.currentTimeMillis() / 60000 * 60000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams = new ResponsiveKafkaStreams(
            builder.build(), properties
        );
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // interleave events in the first window
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 0, 2, 0, 1);
      timestamp.set(baseTs + 50);
      pipeInput(producer, otherTopic(), () -> timestamp.getAndAdd(50), 0, 2, 0, 1);

      latch1.await();
    }

    // force a commit/flush so that we can test Cassandra by closing
    // the old Kafka Streams and creating a new one
    properties.put(APPLICATION_SERVER_CONFIG, "host2:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // add one more event to each window
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 2, 3, 0, 1);
      timestamp.set(baseTs + 250);
      pipeInput(producer, otherTopic(), () -> timestamp.getAndAdd(50), 2, 3, 0, 1);

      // add two events in a different window
      timestamp.set(baseTs + 10000);
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(10), 100, 101, 0, 1);
      pipeInput(producer, otherTopic(), () -> timestamp.getAndAdd(10), 100, 101, 0, 1);

      latch2.await();
    }

    // Then:
    assertThat(collect.size(), Matchers.is(2));

    final Queue<Long> k0 = collect.get(0L);
    assertThat(k0, Matchers.containsInAnyOrder(
        0L,
        1L,
        2L,
        1L << Integer.SIZE,
        1L << Integer.SIZE | 1L,
        1L << Integer.SIZE | 2L,
        2L << Integer.SIZE,
        2L << Integer.SIZE | 1L,
        2L << Integer.SIZE | 2L,
        100L << Integer.SIZE | 100L
    ));
  }

  private Windowed<Long> windowed(final long k, final long startMs, final long size) {
    return new Windowed<>(k, new TimeWindow(startMs, startMs + size));
  }

  private Windowed<String> windowedKeyInMinutes(
      final long startMinutes,
      final long endMinutes
  ) {
    final long startMs = minutesToMillis(startMinutes);
    final long endMs = minutesToMillis(endMinutes);
    return new Windowed<>("key", new TimeWindow(startMs, endMs));
  }

  private Windowed<String> windowedKey(final long startMs) {
    return new Windowed<>("key", new TimeWindow(startMs, startMs + 10_000));
  }

  private static void pipeInput(
      final KafkaProducer<Long, Long> producer,
      final String topic,
      final Supplier<Long> timestamp,
      final long valFrom,
      final long valTo,
      final long... keys
  ) {
    for (final long k : keys) {
      for (long v = valFrom; v < valTo; v++) {
        producer.send(new ProducerRecord<>(
            topic,
            0,
            timestamp.get(),
            k,
            v
        ));
      }
    }
    producer.flush();
  }

  private Map<String, Object> getMutablePropertiesWithStringSerdes() {
    final Map<String, Object> properties = getMutableProperties();
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
    return properties;
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
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1); // commit as often as possible

    properties.put(consumerPrefix(REQUEST_TIMEOUT_MS_CONFIG), 5_000);
    properties.put(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 5_000 - 1);

    properties.put(consumerPrefix(MAX_POLL_RECORDS_CONFIG), 1);

    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);

    return properties;
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