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

import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.TENANT_ID_CONFIG;
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
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.ResponsiveStores;
import dev.responsive.kafka.api.ResponsiveWindowedStoreSupplier;
import dev.responsive.kafka.config.ResponsiveDriverConfig;
import dev.responsive.utils.ContainerExtension;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;

@ExtendWith(ContainerExtension.class)
public class ResponsiveWindowIntegrationTest {

  private static final String INPUT_TOPIC = "input";
  private static final String OTHER_TOPIC = "other";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private String bootstrapServers;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      final KafkaContainer kafka
  ) throws InterruptedException, ExecutionException {
    name = info.getTestMethod().orElseThrow().getName();
    bootstrapServers = kafka.getBootstrapServers();

    responsiveProps.put(STORAGE_HOSTNAME_CONFIG, cassandra.getContactPoint().getHostName());
    responsiveProps.put(STORAGE_PORT_CONFIG, cassandra.getContactPoint().getPort());
    responsiveProps.put(STORAGE_DATACENTER_CONFIG, cassandra.getLocalDatacenter());
    responsiveProps.put(TENANT_ID_CONFIG, "responsive_clients");   // keyspace is expected to exist
    responsiveProps.put(INTERNAL_TASK_ASSIGNOR_CLASS, StickyTaskAssignor.class.getName());

    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));

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
    admin.close();
  }

  @Test
  public void shouldComputeWindowedAggregateWithRetention() throws InterruptedException {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final StreamsBuilder builder = new StreamsBuilder();

    final ConcurrentMap<Windowed<Long>, Long> collect = new ConcurrentHashMap<>();
    final CountDownLatch latch1 = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(2);
    final KStream<Long, Long> input = builder.stream(inputTopic());
    input.groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
        .aggregate(
            () -> 0L,
            (k, v, agg) -> agg + v,
            ResponsiveStores.windowMaterialized(
                name,
                6_000,
                5_000,
                false
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
        .to(outputTopic());

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = System.currentTimeMillis() / 60000 * 60000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams =
            ResponsiveKafkaStreams.create(builder.build(), properties);
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();

      // produce two keys each having values 0-10 that are 1s apart
      // which should result in two windows where 4 is the max of the
      // first and 9 is the max of the second - we need to produce them
      // 5 at a time to make sure that the timestamp hasn't moved past
      // retention (and therefore events from the second key would
      // be dropped)
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 0, 3, 0, 1);
      latch1.await();
    }

    // force a commit/flush so that we can test Cassandra by closing
    // the old Kafka Streams and creating a new one
    properties.put(APPLICATION_SERVER_CONFIG, "host2:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams = ResponsiveKafkaStreams.create(
            builder.build(),
            properties
        );
        final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)
    ) {
      kafkaStreams.start();
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 3, 5, 0, 1);

      timestamp.set(baseTs + 10000);
      pipeInput(producer, inputTopic(), () -> timestamp.getAndAdd(100), 5, 10, 0, 1);

      // at this point the records produced by this pipe should be
      // past the retention period and therefore be ignored from the
      // first window
      timestamp.set(baseTs);
      pipeInput(producer, inputTopic(), timestamp::get, 100, 101, 0, 1);

      // use this to signify that we're done processing and count
      // down the latch
      timestamp.set(baseTs + 15000);
      pipeInput(producer, inputTopic(), timestamp::get, 1000, 1001, 0, 1);
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
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(inputTopic());
    final KStream<Long, Long> other = builder.stream(otherTopic());
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
                    "input" + name, 1000, 1000, true),
                new ResponsiveWindowedStoreSupplier(
                    "other" + name, 1000, 1000, true)
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
        .to(outputTopic());

    // When:
    // use a base timestamp that is aligned with a minute boundary to
    // ensure predictable test results
    final long baseTs = System.currentTimeMillis() / 60000 * 60000;
    final AtomicLong timestamp = new AtomicLong(baseTs);
    properties.put(APPLICATION_SERVER_CONFIG, "host1:1024");
    try (
        final ResponsiveKafkaStreams kafkaStreams = ResponsiveKafkaStreams.create(
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
            ResponsiveKafkaStreams.create(builder.build(), properties);
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
    final Map<String, Object> properties = new HashMap<>(responsiveProps);

    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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

    properties.put(ResponsiveDriverConfig.STORE_FLUSH_RECORDS_THRESHOLD, 1);

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