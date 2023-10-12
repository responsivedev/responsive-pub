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

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.SUBPARTITION_HASHER_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.responsiveConfig;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraFactTable;
import dev.responsive.kafka.internal.db.CassandraKeyValueTable;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.partitioning.Hasher;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.BaseTableSpec;
import dev.responsive.kafka.internal.stores.ResponsiveMaterialized;
import dev.responsive.kafka.internal.utils.TableName;
import dev.responsive.kafka.testutils.IntegrationTestUtils;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
public class SubPartitionIntegrationTest {

  private static final int FLUSH_THRESHOLD = 1;
  private static final long MIN_VALID_TS = 0L;

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private String storeName;
  private Admin admin;
  private CassandraClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps,
      final CassandraContainer<?> cassandra
  ) throws ExecutionException, InterruptedException {
    name = info.getTestMethod().orElseThrow().getName();
    storeName = name + "store";

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(2), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    ).all().get();

    final CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, responsiveConfig(responsiveProps));
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), outputTopic()));
  }

  @Test
  public void shouldFlushToRemoteEvery10kRecords() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final var streams = new ResponsiveKafkaStreams(
            simpleDslTopology(ResponsiveStores.materialized(
                ResponsiveKeyValueParams.keyValue(storeName))), properties);
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer();
    ) {
      // When:
      // this will send one key to each virtual partition using the LongBytesHasher
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(20), streams);
      IntegrationTestUtils.pipeInput(
          inputTopic(), 2, producer, System::currentTimeMillis, 0, 100L,
          LongStream.range(0, 32).toArray());

      // Then
      IntegrationTestUtils.awaitOutput(
          outputTopic(),
          0,
          LongStream.range(0, 32)
              .boxed()
              .map(k -> new KeyValue<>(k, 100L))
              .collect(Collectors.toSet()),
          true,
          properties);
      final String cassandraName = new TableName(storeName).remoteName();
      final RemoteKVTable table = CassandraKeyValueTable.create(
          new BaseTableSpec(cassandraName), client);

      assertThat(client.numPartitions(cassandraName), is(OptionalInt.of(32)));
      assertThat(client.count(cassandraName, 0), is(2L));
      assertThat(client.count(cassandraName, 16), is(2L));

      // these store ValueAndTimestamp, so we need to just pluck the last 8 bytes
      final var hasher = new SubPartitioner(16, new LongBytesHasher());
      for (long k = 0; k < 32; k++) {
        final var kBytes = Bytes.wrap(serializer.serialize("", k));
        assertThat(
            deserializer.deserialize("foo",
                Arrays.copyOfRange(
                    table.get(hasher.partition((int) (k % 2), kBytes), kBytes,
                        MIN_VALID_TS),
                    8,
                    16)),
            is(100L));
      }
    }
  }

  @Test
  public void shouldIgnoreSubPartitionerOnFactSchema() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final var streams = new ResponsiveKafkaStreams(
            simpleDslTopology(new ResponsiveMaterialized<>(
                Materialized.as(ResponsiveStores.factStore(storeName)),
                false
            )), properties);
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer();
    ) {
      // When:
      // this will send one key to each virtual partition using the LongBytesHasher
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      IntegrationTestUtils.pipeInput(
          inputTopic(), 2, producer, System::currentTimeMillis, 0, 100L,
          LongStream.range(0, 32).toArray());

      // Then
      IntegrationTestUtils.awaitOutput(
          outputTopic(),
          0,
          LongStream.range(0, 32)
              .boxed()
              .map(k -> new KeyValue<>(k, 100L))
              .collect(Collectors.toSet()),
          true,
          properties);
      final String cassandraName = new TableName(storeName).remoteName();
      final RemoteKVTable table = CassandraFactTable.create(
          new BaseTableSpec(cassandraName), client);

      final var meta0 = table.metadata(0);
      final var meta1 = table.metadata(1);

      assertThat(meta0.offset, is(notNullValue()));
      assertThat(meta1.offset, is(notNullValue()));

      // throws because it doesn't exist
      Assertions.assertEquals(table.metadata(2).offset, NO_COMMITTED_OFFSET);

      // these store ValueAndTimestamp, so we need to just pluck the last 8 bytes
      final var hasher = SubPartitioner.NO_SUBPARTITIONS;
      for (long k = 0; k < 32; k++) {
        final var kBytes = Bytes.wrap(serializer.serialize("", k));
        assertThat(
            deserializer.deserialize("foo",
                Arrays.copyOfRange(
                    table.get(
                        hasher.partition((int) (k % 2), kBytes),
                        kBytes,
                        MIN_VALID_TS),
                    8,
                    16)),
            is(100L));
      }
    }
  }

  private Topology simpleDslTopology(
      final Materialized<Long, Long, KeyValueStore<Bytes, byte[]>> materialized
  ) {
    final var builder = new StreamsBuilder();

    final KStream<Long, Long> stream = builder.stream(inputTopic());
    stream.groupByKey()
        .count(materialized)
        .toStream()
        .to(outputTopic());

    return builder.build();
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
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    properties.put(STORAGE_DESIRED_NUM_PARTITION_CONFIG, 32);
    properties.put(STORE_FLUSH_RECORDS_TRIGGER_CONFIG, FLUSH_THRESHOLD);
    properties.put(SUBPARTITION_HASHER_CONFIG, LongBytesHasher.class);

    return properties;
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  public static class LongBytesHasher implements Hasher {

    final LongDeserializer deserializer = new LongDeserializer();

    @Override
    public Integer apply(final Bytes bytes) {
      return Optional.ofNullable(deserializer.deserialize("ignored", bytes.get()))
          .map(k -> k / 2) // we want 0 -> 0, 1 -> 16, 2 -> 1, 3 -> 17 ...
          .map(Long::intValue)
          .orElse(null);
    }
  }
}
