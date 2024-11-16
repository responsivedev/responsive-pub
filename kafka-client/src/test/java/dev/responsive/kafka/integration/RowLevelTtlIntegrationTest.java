/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.integration;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutputWithTimestamps;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static dev.responsive.kafka.testutils.processors.Deduplicator.deduplicatorApp;
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

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.TtlProvider;
import dev.responsive.kafka.api.stores.TtlProvider.TtlDuration;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RowLevelTtlIntegrationTest {

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final String STORE_NAME = "store";

  private static final Duration DEFAULT_TTL = Duration.ofSeconds(10);

  private static final String KEEP_FOREVER = "KEEP_FOREVER";
  private static final String TTL_5s = "TTL_5s";
  private static final String TTL_20s = "TTL_20s";

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
  public void shouldApplyRowLevelTtlForKeyAndValue() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (final ResponsiveKafkaStreams streams = buildStreams(properties)) {
      startAppAndAwaitRunning(Duration.ofSeconds(20), streams);

      // First pipe the "base data" all at t=0, all of which should produce output
      pipeTimestampedRecords(producer, inputTopic(), List.of(
          new KeyValueTimestamp<>(KEEP_FOREVER, "ignored", 0L),
          new KeyValueTimestamp<>(TTL_5s, "ignored", 0L),
          new KeyValueTimestamp<>(TTL_20s, "ignored", 0L),
          new KeyValueTimestamp<>("ignored_key(ttl=infinite)", KEEP_FOREVER, 0L),
          new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 0L),
          new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 0L),
          new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 0L),

          // Then start testing ttl deduplicator by advancing streamTime and sending duplicate keys
          // Now only records whose previous value has expired should produce output

          // streamTime = 6s
          new KeyValueTimestamp<>(KEEP_FOREVER, "ignored", 6_000L),
          new KeyValueTimestamp<>(TTL_5s, "ignored", 6_000L), // forwarded
          new KeyValueTimestamp<>(TTL_20s, "ignored", 6_000L),
          new KeyValueTimestamp<>("ignored_key(ttl=infinite)", KEEP_FOREVER, 6_000L),
          new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 6_000L), // forwarded
          new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 6_000L),
          new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 6_000L),


          // streamTime = 12s
          new KeyValueTimestamp<>(KEEP_FOREVER, "ignored", 12_000L),
          new KeyValueTimestamp<>(TTL_5s, "ignored", 12_000L), // forwarded
          new KeyValueTimestamp<>(TTL_20s, "ignored", 12_000L),
          new KeyValueTimestamp<>("ignored_key(ttl=infinite)", KEEP_FOREVER, 12_000L),
          new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 12_000L), // forwarded
          new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 12_000L),
          new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 12_000L), // forwarded

          // streamTime = 300s
          new KeyValueTimestamp<>(KEEP_FOREVER, "ignored", 300_000L),
          new KeyValueTimestamp<>(TTL_5s, "ignored", 300_000L), // forwarded
          new KeyValueTimestamp<>(TTL_20s, "ignored", 300_000L), // forwarded
          new KeyValueTimestamp<>("ignored_key(ttl=infinite)", KEEP_FOREVER, 300_000L),
          new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 300_000L), // forwarded
          new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 300_000L), // forwarded
          new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 300_000L) // forwarded
      ));

      final var kvs = readOutputWithTimestamps(outputTopic(), 0, 17, 1, true, properties);
      assertThat(
          kvs,
          equalTo(List.of(
              // base records (streamTime = 0)
              new KeyValueTimestamp<>(KEEP_FOREVER, "ignored", 0L),
              new KeyValueTimestamp<>(TTL_5s, "ignored", 0L),
              new KeyValueTimestamp<>(TTL_20s, "ignored", 0L),
              new KeyValueTimestamp<>("ignored_key(ttl=infinite)", KEEP_FOREVER, 0L),
              new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 0L),
              new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 0L),
              new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 0L),
              // first wave (streamTime = 6s)
              new KeyValueTimestamp<>(TTL_5s, "ignored", 6_000L),
              new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 6_000L),
              // second wave (streamTime = 12s)
              new KeyValueTimestamp<>(TTL_5s, "ignored", 12_000L),
              new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 12_000L),
              new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 12_000L),
              // third wave (streamTime = 300s)
              new KeyValueTimestamp<>(TTL_5s, "ignored", 300_000L),
              new KeyValueTimestamp<>(TTL_20s, "ignored", 300_000L),
              new KeyValueTimestamp<>("ignored_key(ttl=5s)", TTL_5s, 300_000L),
              new KeyValueTimestamp<>("ignored_key(ttl=20s)", TTL_20s, 300_000L),
              new KeyValueTimestamp<>("ignored_key(defaultTtl=10s)", "ignored", 300_000L)
          )
      ));
    }
  }

  private ResponsiveKafkaStreams buildStreams(final Map<String, Object> properties) {
    final var params = ResponsiveKeyValueParams.fact(STORE_NAME).withTtlProvider(
        TtlProvider.<String, ValueAndTimestamp<String>>withDefault(DEFAULT_TTL)
            .fromKeyAndValue(RowLevelTtlIntegrationTest::ttlForKeyAndValue)
    );
    final Topology topology = deduplicatorApp(inputTopic(), outputTopic(), params);

    return new ResponsiveKafkaStreams(topology, properties);
  }

  private static Optional<TtlDuration> ttlForKeyAndValue(
      final String key,
      final ValueAndTimestamp<String> valueAndTimestamp
  ) {
    // key takes precedence when resolving ttl
    switch (key) {
      case KEEP_FOREVER:
        return Optional.of(TtlDuration.infinite());
      case TTL_5s:
        return Optional.of(TtlDuration.of((Duration.ofSeconds(5))));
      case TTL_20s:
        return Optional.of(TtlDuration.of((Duration.ofSeconds(20))));
      default:
        // do nothing
    }

    // if key does not specify ttl then value is checked
    final String value = valueAndTimestamp.value();
    switch (value) {
      case KEEP_FOREVER:
        return Optional.of(TtlDuration.infinite());
      case TTL_5s:
        return Optional.of(TtlDuration.of((Duration.ofSeconds(5))));
      case TTL_20s:
        return Optional.of(TtlDuration.of((Duration.ofSeconds(20))));
      default:
        // do nothing
    }

    // finally, just fall back to default ttl (10s)
    return Optional.empty();
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
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 0);

    properties.put(
        ResponsiveConfig.MONGO_ADDITIONAL_CONNECTION_STRING_PARAMS_CONFIG, "maxIdleTimeMs=60000"
    );

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    return properties;
  }

}