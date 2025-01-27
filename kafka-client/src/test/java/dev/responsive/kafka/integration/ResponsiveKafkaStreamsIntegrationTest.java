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

import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeTimestampedRecords;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.mongo.OrderPreservingBase64Encoder;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.KeyValueTimestamp;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.MongoDBContainer;

public class ResponsiveKafkaStreamsIntegrationTest {

  public static final String COUNT_TABLE_NAME = "count";
  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private MongoDBContainer mongo;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      final MongoDBContainer mongo,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    // add displayName to name to account for parameterized tests
    name = info.getTestMethod().orElseThrow().getName() + "-" + new Random().nextInt();
    this.mongo = mongo;

    this.responsiveProps.putAll(responsiveProps);

    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  @Test
  public void shouldDefaultToResponsiveStoresWhenUsingDsl() throws Exception {
    // Given:
    final List<KeyValueTimestamp<String, String>> inputEvents = Arrays.asList(
        new KeyValueTimestamp<>("key", "a", 0L),
        new KeyValueTimestamp<>("key", "b", 2_000L),
        new KeyValueTimestamp<>("key", "c", 3_000L),
        new KeyValueTimestamp<>("STOP", "ignored", 18_000L)
    );
    final CountDownLatch outputLatch = new CountDownLatch(1);

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> input = builder.stream(inputTopic());
    input
        .groupByKey()
        .count(Named.as(COUNT_TABLE_NAME))
        .toStream()
        .peek((k, v) -> {
          if (k.equals("STOP")) {
            outputLatch.countDown();
          }
        })
        .selectKey((k, v) -> k)
        .to(outputTopic());

    // When:
    final Map<String, Object> properties =
        IntegrationTestUtils.getDefaultMutablePropertiesWithStringSerdes(responsiveProps, name);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            new ResponsiveKafkaStreams(builder.build(), properties)
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(15), kafkaStreams);
      pipeTimestampedRecords(producer, inputTopic(), inputEvents);

      final long maxWait = inputEvents.get(inputEvents.size() - 1).timestamp() + 2_000;
      assertThat(
          outputLatch.await(maxWait, TimeUnit.MILLISECONDS),
          Matchers.equalTo(true)
      );
    }

    // Then:
    try (
        final var mongoClient = SessionUtil.connect(mongo.getConnectionString(), "", null);
        final var deserializer = new StringDeserializer();
    ) {
      final List<String> dbs = new ArrayList<>();
      mongoClient.listDatabaseNames().into(dbs);
      assertThat(dbs, hasItem("kstream_aggregate_state_store_0000000001"));

      final var db = mongoClient.getDatabase("kstream_aggregate_state_store_0000000001");
      final var collection = db.getCollection("kv_data");
      final long numDocs = collection.countDocuments();
      assertThat(numDocs, is(2L));

      final List<String> keys = new ArrayList<>();
      collection.find()
          .map(doc -> doc.get("_id", String.class))
          .map(idStr -> new OrderPreservingBase64Encoder().decode(idStr))
          .map(id -> deserializer.deserialize("", id))
          .into(keys);
      assertThat(keys, hasItems("key", "STOP"));
    }
  }

}
