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

import static dev.responsive.kafka.config.ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveConfig.SUBPARTITION_HASHER_CONFIG;
import static dev.responsive.utils.IntegrationTestUtils.awaitOutput;
import static dev.responsive.utils.IntegrationTestUtils.pipeInput;
import static dev.responsive.utils.IntegrationTestUtils.startAppAndAwaitRunning;
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

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraKeyValueTable;
import dev.responsive.db.RemoteKeyValueTable;
import dev.responsive.db.partitioning.Hasher;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.ResponsiveStores;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.utils.ResponsiveConfigParam;
import dev.responsive.utils.ResponsiveExtension;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.CassandraContainer;

@ExtendWith(ResponsiveExtension.class)
public class SubPartitionIntegrationTest {

  private static final int FLUSH_THRESHOLD = 1;

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final String STORE_NAME = "store-Name"; // use non-valid cassandr67TY89U0PIKAT

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
  ) {
    name = info.getTestMethod().orElseThrow().getName();
    storeName = name + "store";

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(2), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty())
        )
    );

    final CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, new ResponsiveConfig(responsiveProps));
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
        final var streams = ResponsiveKafkaStreams.create(simpleDslTopology(), properties);
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer();
    ) {
      // When:
      // this will send one key to each virtual partition using the LongBytesHasher
      startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      pipeInput(
          inputTopic(), 2, producer, System::currentTimeMillis, 0, 100L,
          LongStream.range(0, 32).toArray());

      // Then
      awaitOutput(
          outputTopic(),
          0,
          LongStream.range(0, 32)
              .boxed()
              .map(k -> new KeyValue<>(k, 100L))
              .collect(Collectors.toSet()),
          true,
          properties);
      final String cassandraName = new TableName(storeName).cassandraName();
      final RemoteKeyValueTable statements = new CassandraKeyValueTable(client);
      statements.prepare(cassandraName);

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
                    statements.get(cassandraName, hasher.partition((int) (k % 2), kBytes), kBytes),
                    8,
                    16)),
            is(100L));
      }
    }
  }

  private Topology simpleDslTopology() {
    final var builder = new StreamsBuilder();

    final KStream<Long, Long> stream = builder.stream(inputTopic());
    stream.groupByKey()
        .count(ResponsiveStores.materialized(storeName))
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
