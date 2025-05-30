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

import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_LICENSE_SERVER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.getDefaultMutablePropertiesWithStringSerdes;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.LicenseUtils;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import dev.responsive.kafka.testutils.TestLicenseServer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class OriginEventIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private TestLicenseServer licenseServer;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps,
      final TestLicenseServer licenseServer
  ) throws ExecutionException, InterruptedException {
    name = IntegrationTestUtils.getCassandraValidName(info);
    this.licenseServer = licenseServer;
    this.responsiveProps.putAll(responsiveProps);

    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(2), Optional.empty()),
            new NewTopic(inputTopicTable(), Optional.of(2), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String inputTopicTable() {
    return name + ".table." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldCountOriginEventsSimpleTopology(final boolean eos) throws Exception {
    // Given:
    responsiveProps.put(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        eos ? StreamsConfig.EXACTLY_ONCE_V2 : StreamsConfig.AT_LEAST_ONCE
    );

    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(numEvents);
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic())
        .peek((k, v) -> latch.countDown())
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i), i))
        .collect(Collectors.toList());

    // When:
    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }

    // Then:
    final String appId = (String) props.get(StreamsConfig.APPLICATION_ID_CONFIG);
    assertThat(licenseServer.eventCounts().get(appId), is((long) numEvents));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldNotDoubleCountRepartitionedEvents(final boolean eos) throws Exception {
    // Given:
    responsiveProps.put(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        eos ? StreamsConfig.EXACTLY_ONCE_V2 : StreamsConfig.AT_LEAST_ONCE
    );

    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(numEvents);
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic())
        .repartition(Repartitioned.numberOfPartitions(4))
        .peek((k, v) -> latch.countDown())
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i), i))
        .collect(Collectors.toList());

    // When:
    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }

    // Then:
    final String appId = (String) props.get(StreamsConfig.APPLICATION_ID_CONFIG);
    assertThat(licenseServer.eventCounts().get(appId), is((long) numEvents));
  }

  @Test
  public void shouldNotDoubleCountAggregatedEvents() throws Exception {
    // Given:
    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(20); // twenty keys with 5 count each
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic())
        .groupBy((k, v) -> v)
        .count()
        .toStream()
        // we want a sub-topology downstream of agg
        .repartition(Repartitioned.numberOfPartitions(4))
        .peek((k, v) -> {
          if (v == 5) {
            latch.countDown();
          }
        })
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i % 20), i))
        .collect(Collectors.toList());

    // When:
    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }

    // Then:
    final String appId = (String) props.get(StreamsConfig.APPLICATION_ID_CONFIG);
    assertThat(licenseServer.eventCounts().get(appId), is((long) numEvents));
  }

  @Test
  public void shouldNotDoubleCountWindowedAggregatedEvents() throws Exception {
    // Given:
    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(20); // 2 keys * 10 buckets = 20
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> stream = builder.stream(inputTopic());
    final KStream<Windowed<String>, String> windowedStream = stream.groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(10)))
        .count()
        .mapValues(String::valueOf)
        .toStream();

    // we want a sub-topology downstream of agg
    windowedStream
        .repartition(Repartitioned.with(
            WindowedSerdes.timeWindowedSerdeFrom(String.class, 10),
            Serdes.String()
        ))
        .peek((k, v) -> {
          if ("5".equalsIgnoreCase(v)) {
            latch.countDown();
          }
        })
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i % 2, String.valueOf(i), i))
        .collect(Collectors.toList());

    // When:
    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }

    // Then:
    final String appId = (String) props.get(StreamsConfig.APPLICATION_ID_CONFIG);
    assertThat(licenseServer.eventCounts().get(appId), is((long) numEvents));
  }

  @Test
  public void shouldNotDoubleCountJoinedEvents() throws Exception {
    // Given:
    final int eventsPerTopic = 100;
    final CountDownLatch latch = new CountDownLatch(eventsPerTopic);
    final StreamsBuilder builder = new StreamsBuilder();
    final KTable<String, String> table =
        builder.table(inputTopicTable(), Materialized.as("table1"));
    final KStream<String, String> stream = builder.stream(inputTopic());
    stream
        // use left join to avoid races populating the table
        .leftJoin(table, (v1, v2) -> v1 + (v2 == null ? "-none" : "-joined"))
        // we want a sub-topology downstream of join
        .repartition(Repartitioned.numberOfPartitions(4))
        .peek((k, v) -> latch.countDown())
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputsLeft = IntStream.range(0, eventsPerTopic)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i), eventsPerTopic + i))
        .collect(Collectors.toList());

    final List<KeyValueTimestamp<String, String>> inputsRight = IntStream.range(0, eventsPerTopic)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, "val", i))
        .collect(Collectors.toList());

    // When:
    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopicTable(), inputsRight);
      pipeTimestampedRecords(producer, inputTopic(), inputsLeft);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }

    // Then:
    final String appId = (String) props.get(StreamsConfig.APPLICATION_ID_CONFIG);
    assertThat(licenseServer.eventCounts().get(appId), is(2L * eventsPerTopic));
  }

  @Test
  public void shouldNotReportIfUsingTrialLicenseType() throws Exception {
    // Given:
    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(numEvents);
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic())
        .peek((k, v) -> latch.countDown())
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i), i))
        .collect(Collectors.toList());

    // Then (No Error):
    responsiveProps.put(RESPONSIVE_LICENSE_CONFIG, LicenseUtils.getTrialLicense());
    responsiveProps.put(RESPONSIVE_LICENSE_SERVER_CONFIG, "CRASH_IF_LOAD_SERVER");

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }
  }

  @Test
  public void shouldNotReportIfUsingCloudLicense() throws Exception {
    // Given:
    final int numEvents = 100;
    final CountDownLatch latch = new CountDownLatch(numEvents);
    final StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(inputTopic())
        .peek((k, v) -> latch.countDown())
        .to(outputTopic());

    final List<KeyValueTimestamp<String, String>> inputs = IntStream.range(0, numEvents)
        .mapToObj(i -> new KeyValueTimestamp<>("key" + i, String.valueOf(i), i))
        .collect(Collectors.toList());

    // Then (No Error):
    responsiveProps.remove(RESPONSIVE_LICENSE_CONFIG);
    responsiveProps.put(ResponsiveConfig.PLATFORM_API_KEY_CONFIG, "somevalue");
    responsiveProps.put(ResponsiveConfig.PLATFORM_API_SECRET_CONFIG, "somevalue");
    responsiveProps.put(RESPONSIVE_LICENSE_SERVER_CONFIG, "CRASH_IF_LOAD_SERVER");

    final var props = getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    try (final var kafkaStreams = new ResponsiveKafkaStreams(builder.build(), props)) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputs);
      assertThat(latch.await(30, TimeUnit.SECONDS), is(true));
    }
  }

}
