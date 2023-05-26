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

package dev.responsive.kafka.store;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.TestConstants;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.ResponsiveWindowedStoreSupplier;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KafkaStreams;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ResponsiveWindowIntegrationTest {

  private static final String INPUT_TOPIC = "input";
  private static final String OTHER_TOPIC = "other";
  private static final String OUTPUT_TOPIC = "output";

  private Admin admin;
  private CqlSession session;
  private CassandraClient client;
  private ScheduledExecutorService executor;

  @Container
  public CassandraContainer<?> cassandra = new CassandraContainer<>(TestConstants.CASSANDRA)
      .withInitScript("CassandraDockerInit.cql");
  @Container
  public KafkaContainer kafka = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000");

  private String name;

  @BeforeEach
  public void before(final TestInfo info) {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session);
    executor = new ScheduledThreadPoolExecutor(2);
    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));

    admin.createTopics(
        List.of(
            new NewTopic(INPUT_TOPIC, Optional.of(1), Optional.empty()),
            new NewTopic(OTHER_TOPIC, Optional.of(1), Optional.empty()),
            new NewTopic(OUTPUT_TOPIC, Optional.of(1), Optional.empty())
        )
    );
  }

  @AfterEach
  public void after() {
    session.close();
    executor.shutdown();
    admin.deleteTopics(List.of(INPUT_TOPIC, OTHER_TOPIC, OUTPUT_TOPIC));
    admin.close();
  }

  @Test
  public void shouldComputeWindowedAggregateWithRetention() throws InterruptedException {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final StreamsConfig config = new StreamsConfig(properties);
    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<Long>, Long> collect = new ConcurrentHashMap<>();
    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(2);
    final KStream<Long, Long> input = builder.stream(INPUT_TOPIC);
    input.groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
        .aggregate(
            () -> 0L,
            (k, v, agg) -> agg + v,
            Materialized.as(
                new ResponsiveWindowedStoreSupplier(
                    client,
                    name,
                    executor,
                    admin,
                    6_000,
                    5_000,
                    false
                )
            ))
        .toStream()
        .peek((k, v) -> {
          collect.put(k, v);

          if (v == 3) {
            latch1.countDown();
          } else if (v == 1000) {
            latch2.countDown();
          }
        })
        // discard the window, so we don't have to serialize it
        // we're not checking the output topic anyway
        .map((k, v) -> new KeyValue<>(k.key(), v))
        .to(OUTPUT_TOPIC);

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = System.currentTimeMillis() / 60000 * 60000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // produce two keys each having values 0-10 that are 1s apart
      // which should result in two windows where 4 is the max of the
      // first and 9 is the max of the second - we need to produce them
      // 5 at a time to make sure that the timestamp hasn't moved past
      // retention (and therefore events from the second key would
      // be dropped)
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(100), 0, 3, 0, 1);
      latch1.await();
    }

    // force a commit/flush so that we can test Cassandra by closing
    // the old Kafka Streams and creating a new one
    properties.put(APPLICATION_SERVER_CONFIG, "host2:1024");
    try (
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(100), 3, 5, 0, 1);

      timestamp.set(baseTs + 10000);
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(100), 5, 10, 0, 1);

      // at this point the records produced by this pipe should be
      // past the retention period and therefore be ignored from the
      // first window
      timestamp.set(baseTs);
      pipeInput(producer, INPUT_TOPIC, timestamp::get, 100, 101, 0, 1);

      // use this to signify that we're done processing and count
      // down the latch
      timestamp.set(baseTs + 15000);
      pipeInput(producer, INPUT_TOPIC, timestamp::get, 1000, 1001, 0, 1);
      latch2.await();
    }

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
  public void shouldComputeWindowedJoinUsingRanges() throws InterruptedException {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final StreamsConfig config = new StreamsConfig(properties);
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(INPUT_TOPIC);
    final KStream<Long, Long> other = builder.stream(OTHER_TOPIC);
    final Map<Long, Queue<Long>> collect = new ConcurrentHashMap<>();

    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(2);
    input.join(other,
            (v1, v2) -> {
              System.out.println("Joining: " + v1 + ", " + v2);
              return (v1 << Integer.SIZE) | v2;
            },
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(500)),
            StreamJoined.with(
                new ResponsiveWindowedStoreSupplier(
                    client, "input" + name, executor, admin, 1000, 1000, true),
                new ResponsiveWindowedStoreSupplier(
                    client, "other" + name, executor, admin, 1000, 1000, true)
            ))
        .peek((k, v) -> {
          collect.computeIfAbsent(k, old -> new ArrayBlockingQueue<>(10)).add(v);
          if (v.intValue() == 1) {
            latch1.countDown();
          }
          if (v.intValue() == 100) {
            latch2.countDown();
          }
        })
        .to(OUTPUT_TOPIC);

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = System.currentTimeMillis() / 60000 * 60000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // interleave events in the first window
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(100), 0, 2, 0, 1);
      timestamp.set(baseTs + 50);
      pipeInput(producer, OTHER_TOPIC, () -> timestamp.getAndAdd(50), 0, 2, 0, 1);

      latch1.await();
    }

    // force a commit/flush so that we can test Cassandra by closing
    // the old Kafka Streams and creating a new one
    properties.put(APPLICATION_SERVER_CONFIG, "host2:1024");
    try (
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // add one more event to each window
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(100), 2, 3, 0, 1);
      timestamp.set(baseTs + 250);
      pipeInput(producer, OTHER_TOPIC, () -> timestamp.getAndAdd(50), 2, 3, 0, 1);

      // add two events in a different window
      timestamp.set(baseTs + 10000);
      pipeInput(producer, INPUT_TOPIC, () -> timestamp.getAndAdd(10), 100, 101, 0, 1);
      pipeInput(producer, OTHER_TOPIC, () -> timestamp.getAndAdd(10), 100, 101, 0, 1);

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

  private void pipeInput(
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
  }

  private Map<String, Object> getMutableProperties() {
    final Map<String, Object> properties = new HashMap<>();

    properties.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
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

    return properties;
  }

}