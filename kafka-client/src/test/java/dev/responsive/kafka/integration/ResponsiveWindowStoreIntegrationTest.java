/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.testutils.IntegrationTestUtils.getDefaultMutablePropertiesWithStringSerdes;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.utils.StoreUtil;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
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
  private static final String STOP = "STOP";

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

  @Test
  public void shouldComputeTumblingWindowAggregate() throws Exception {
    // Given:
    final CountDownLatch latch = new CountDownLatch(1);
    final Map<Windowed<String>, String> collect = new ConcurrentHashMap<>();
    final Duration windowSize = Duration.ofSeconds(5);

    final var builder = new StreamsBuilder();
    final KStream<String, String> stream = builder.stream(inputTopic());
    stream
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
        .aggregate(() -> "", (k, v, agg) -> agg + v, Materialized.as(name))
        .toStream()
        .peek(collect::put)
        // discards window for easier serialization since we're not checking
        // the output topic anyway
        .selectKey((k, v) -> k.key())
        .peek((k, v) -> {
          if (k.equals(STOP)) {
            latch.countDown();
          }
        })
        .to(outputTopic());

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);

    // When:
    try (
        final var producer = new KafkaProducer<String, String>(props);
        final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      // final outputs of
      // - [k1, window1] -> "ab",
      // - [k2, window1] -> "ab"
      // - [k1, window2] -> "cd",
      // - [k2, window2] -> "cd"
      pipeTimestampedRecords(producer, inputTopic(), List.of(
          new KeyValueTimestamp<>("k1", "a", 0),
          new KeyValueTimestamp<>("k2", "a", 1),
          new KeyValueTimestamp<>("k1", "b", 2),
          new KeyValueTimestamp<>("k2", "b", 3),

          new KeyValueTimestamp<>("k1", "c", 10_000),
          new KeyValueTimestamp<>("k2", "c", 10_001),
          new KeyValueTimestamp<>("k1", "d", 10_002),
          new KeyValueTimestamp<>("k2", "d", 10_003),

          // this record is for an expired window and therefore should be ignored
          new KeyValueTimestamp<>("k1", "d", 10),

          new KeyValueTimestamp<>(STOP, "", 50_000)
      ));

      // Then:
      assertThat("Latch should have been awaited within 30s", latch.await(30, TimeUnit.SECONDS));

      final long sizeMs = windowSize.toMillis();
      assertThat(collect.entrySet(), hasSize(5));
      assertThat(collect, hasEntry(windowed("k1", 0, sizeMs), "ab"));
      assertThat(collect, hasEntry(windowed("k2", 0, sizeMs), "ab"));
      assertThat(collect, hasEntry(windowed("k1", 10_000, sizeMs), "cd"));
      assertThat(collect, hasEntry(windowed("k2", 10_000, sizeMs), "cd"));
    }
  }

  @Test
  public void shouldTumbleWithSegmentedStore() throws Exception {
    // Given:
    final CountDownLatch latch = new CountDownLatch(1);
    final Map<Windowed<String>, String> collect = new ConcurrentHashMap<>();
    final Duration windowSize = Duration.ofSeconds(5);
    final int numSegments = 5;
    final long segInterval = StoreUtil.computeSegmentInterval(windowSize.toMillis(), numSegments);

    final var builder = new StreamsBuilder();
    final KStream<String, String> stream = builder.stream(inputTopic());
    stream
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
        .aggregate(
            () -> "",
            (k, v, agg) -> agg + v,
            ResponsiveStores.windowMaterialized(
                ResponsiveWindowParams
                    .window(name, windowSize, windowSize, false)
                    .withNumSegments(numSegments))
        )
        .toStream()
        .peek(collect::put)
        // discards window for easier serialization since we're not checking
        // the output topic anyway
        .selectKey((k, v) -> k.key())
        .peek((k, v) -> {
          if (k.equals(STOP)) {
            latch.countDown();
          }
        })
        .to(outputTopic());

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);

    // When:
    try (
        final var producer = new KafkaProducer<String, String>(props);
        final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      // final outputs of
      // - [k1, window1] -> "abc",
      // - [k2, window1] -> "abc"
      // - [k1, window2] -> "de",
      // - [k2, window2] -> "de"
      pipeTimestampedRecords(producer, inputTopic(), List.of(
          // these are within the first segment of the first window
          new KeyValueTimestamp<>("k1", "a", 0),
          new KeyValueTimestamp<>("k2", "a", 1),
          new KeyValueTimestamp<>("k1", "b", 2),
          new KeyValueTimestamp<>("k2", "b", 3),

          // these are within the second segment of the first window
          new KeyValueTimestamp<>("k1", "c", segInterval + 1),
          new KeyValueTimestamp<>("k2", "c", segInterval + 2),

          new KeyValueTimestamp<>("k1", "d", 10_000),
          new KeyValueTimestamp<>("k2", "d", 10_001),
          new KeyValueTimestamp<>("k1", "e", 10_002),
          new KeyValueTimestamp<>("k2", "e", 10_003),

          // this record is for an expired window and therefore should be ignored
          new KeyValueTimestamp<>("k1", "d", 10),

          new KeyValueTimestamp<>(STOP, "", 50_000)
      ));

      // Then:
      assertThat("Latch should have been awaited within 30s", latch.await(30, TimeUnit.SECONDS));

      final long sizeMs = windowSize.toMillis();
      assertThat(collect.entrySet(), hasSize(5));
      assertThat(collect, hasEntry(windowed("k1", 0, sizeMs), "abc"));
      assertThat(collect, hasEntry(windowed("k2", 0, sizeMs), "abc"));
      assertThat(collect, hasEntry(windowed("k1", 10_000, sizeMs), "de"));
      assertThat(collect, hasEntry(windowed("k2", 10_000, sizeMs), "de"));
    }
  }

  @Test
  public void shouldComputeHoppingWindowAggregate() throws Exception {
    // Given:
    final CountDownLatch latch = new CountDownLatch(2); // STOP will be included in two windows
    final Map<Windowed<String>, String> collect = new ConcurrentHashMap<>();
    final Duration windowSize = Duration.ofSeconds(10);
    final Duration grace = Duration.ofSeconds(5);
    final Duration advance = Duration.ofSeconds(5);

    final var builder = new StreamsBuilder();
    final KStream<String, String> stream = builder.stream(inputTopic());
    stream
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, grace).advanceBy(advance))
        .aggregate(() -> "", (k, v, agg) -> agg + v, Materialized.as(name))
        .toStream()
        .peek(collect::put)
        // discards window for easier serialization since we're not checking
        // the output topic anyway
        .selectKey((k, v) -> k.key())
        .peek((k, v) -> {
          if (k.equals(STOP)) {
            latch.countDown();
          }
        })
        .to(outputTopic());

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);

    // When:
    try (
        final var producer = new KafkaProducer<String, String>(props);
        final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), List.of(
          new KeyValueTimestamp<>("key", "a", 0L),
          new KeyValueTimestamp<>("key", "b", 6_000L),
          new KeyValueTimestamp<>("key", "c", 8_000L),
          new KeyValueTimestamp<>("key", "d", 16_000L), // windowCloseTime = 11s --> [0, 10] closed
          new KeyValueTimestamp<>("key", "e", 8_000L),  // within grace for [5, 15s] window
          new KeyValueTimestamp<>("key", "f", 0L),     // outside grace for all windows
          new KeyValueTimestamp<>("key", "g", 11_000L),
          new KeyValueTimestamp<>("key", "h", 5_000L),   // within grace for [5, 15s] window

          new KeyValueTimestamp<>(STOP, "", 45_000)
      ));

      // Then:
      assertThat("Latch should have been awaited within 30s", latch.await(30, TimeUnit.SECONDS));

      assertThat(collect.toString(), collect.entrySet(), hasSize(6)); // STOP has 2 windows
      assertThat(collect, Matchers.hasEntry(windowedKey(0L), "abc"));    // [0, 10s]
      assertThat(collect, Matchers.hasEntry(windowedKey(5_000L), "bcegh")); // [5, 15s]
      assertThat(collect, Matchers.hasEntry(windowedKey(10_000L), "dg"));  // [10s, 20s]
      assertThat(collect, Matchers.hasEntry(windowedKey(15_000L), "d"));   // [15s, 25s]
    }
  }

  @Test
  public void shouldDoStreamStreamJoin() throws Exception {
    // Given:
    final Duration windowSize = Duration.ofSeconds(5);

    final var builder = new StreamsBuilder();
    final KStream<String, String> left = builder.stream(inputTopic());
    final KStream<String, String> right = builder.stream(otherTopic());

    left
        .peek((k, v) -> System.out.println(k + ": " + v))
        .join(
            right,
            (v1, v2) -> v1 + "-" + v2,
            JoinWindows.ofTimeDifferenceWithNoGrace(windowSize.dividedBy(2)),
            StreamJoined.as(name)
        )
        .to(outputTopic());

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    // set the max task idle just in case records come out of order on one topic or the other
    props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 1_000);

    // When:
    try (
        final var producer = new KafkaProducer<String, String>(props);
        final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);

      pipeTimestampedRecords(producer, otherTopic(), List.of(
          new KeyValueTimestamp<>("A", "R:a", 30L),
          new KeyValueTimestamp<>("B", "R:b", 200L), // should join with R:b
          new KeyValueTimestamp<>("B", "R:b2", 500L) // should join with R:b
      ));
      pipeTimestampedRecords(producer, inputTopic(), List.of(
          new KeyValueTimestamp<>("A", "L:a", 0L),
          new KeyValueTimestamp<>("A", "L:a2", 0L),
          new KeyValueTimestamp<>("A", "L:a3", 0L),
          new KeyValueTimestamp<>("B", "L:b", 300L), // should join with R:b
          new KeyValueTimestamp<>("A", "no_match", 2_000L), // should not join, outside of window
          new KeyValueTimestamp<>(STOP, STOP, 10_000L)
      ));

      // Then:
      final List<KeyValue<String, String>> output =
          readOutput(outputTopic(), 0L, 5L, true, props);

      assertThat(output.get(0), equalTo(new KeyValue<>("A", "L:a-R:a")));
      assertThat(output.get(1), equalTo(new KeyValue<>("A", "L:a2-R:a")));
      assertThat(output.get(2), equalTo(new KeyValue<>("A", "L:a3-R:a")));
      assertThat(output.get(3), equalTo(new KeyValue<>("B", "L:b-R:b")));
      assertThat(output.get(4), equalTo(new KeyValue<>("B", "L:b-R:b2")));
    }
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), otherTopic(), outputTopic()));
  }

  private static Windowed<String> windowed(final String k, final long startMs, final long size) {
    return new Windowed<>(k, new TimeWindow(startMs, startMs + size));
  }

  private Windowed<String> windowedKey(final long startMs) {
    return new Windowed<>("key", new TimeWindow(startMs, startMs + 10_000));
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