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

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
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
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraFactTable;
import dev.responsive.kafka.internal.db.CassandraKeyValueTable;
import dev.responsive.kafka.internal.db.partitioning.Hasher;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
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
import java.util.function.Function;
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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;

public class TablePartitionerIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(TablePartitionerIntegrationTest.class);

  @RegisterExtension
  static ResponsiveExtension EXTENSION = new ResponsiveExtension(StorageBackend.CASSANDRA);

  private static final int NUM_PARTITIONS_INPUT = 2;
  private static final int NUM_PARTITIONS_OUTPUT = 1;

  private static final int FLUSH_THRESHOLD = 1;
  private static final long MIN_VALID_TS = 0L;

  private static final long NUM_SEGMENTS = 10L;
  private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);
  private static final Duration GRACE_PERIOD = Duration.ZERO;

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private String storeName;
  private Admin admin;
  private CassandraClient client;
  private ResponsiveConfig config;

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
    config = ResponsiveConfig.responsiveConfig(responsiveProps);

    this.admin = admin;
    admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(NUM_PARTITIONS_INPUT), Optional.empty()),
            new NewTopic(outputTopic(), Optional.of(NUM_PARTITIONS_OUTPUT), Optional.empty())
        )
    ).all().get();

    final CqlSession session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_itests") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session, responsiveConfig(responsiveProps));
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), outputTopic()));
  }

  @Test
  public void shouldFlushToRemoteTableWithSubpartitions() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer()
    ) {
      try (
          final var streams = new ResponsiveKafkaStreams(
              keyValueStoreTopology(ResponsiveKeyValueParams.keyValue(storeName)),
              properties
          )
      ) {
        // When:
        // this will send one key to each virtual partition using the LongBytesHasher
        IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(20), streams);
        IntegrationTestUtils.pipeInput(
            inputTopic(), NUM_PARTITIONS_INPUT, producer, System::currentTimeMillis, 0, 100L,
            LongStream.range(0, 32).toArray()
        );

        // Then
        IntegrationTestUtils.awaitOutput(
            outputTopic(),
            0,
            LongStream.range(0, 32)
                .boxed()
                .map(k -> new KeyValue<>(k, 100L))
                .collect(Collectors.toSet()),
            false,
            properties
        );
      }

      // have this outside the try block so that kafka streams is closed and fully
      // flushed before we assert
      final String cassandraName = new TableName(storeName).tableName();
      final var partitioner = SubPartitioner.create(
          OptionalInt.empty(),
          NUM_PARTITIONS_INPUT,
          cassandraName,
          ResponsiveConfig.responsiveConfig(properties),
          storeName + "-changelog"
      );

      final CassandraKeyValueTable table = CassandraKeyValueTable.create(
          new DefaultTableSpec(cassandraName, partitioner, TtlResolver.NO_TTL, config), client);

      assertThat(client.numPartitions(cassandraName), is(OptionalInt.of(32)));
      assertThat(client.count(cassandraName, 0), is(2L));
      assertThat(client.count(cassandraName, 16), is(2L));

      LOG.info("Checking data in remote table");
      for (long tp = 0; tp < 32; ++tp) {
        final var kBytes = Bytes.wrap(serializer.serialize("", tp));
        final byte[] valBytes = table.get((int) tp % NUM_PARTITIONS_INPUT, kBytes, MIN_VALID_TS);
        final Long val = deserializer.deserialize("foo", Arrays.copyOfRange(valBytes, 8, 16));
        assertThat(val, is(100L));
      }
    }
  }

  @Test
  public void shouldFlushToRemoteTableWithoutSubpartitions() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    try (
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer()
    ) {
      try (
          final var streams = new ResponsiveKafkaStreams(
              keyValueStoreTopology(ResponsiveKeyValueParams.fact(storeName)),
              properties
          )
      ) {
        // When:
        // this will send one key to each virtual partition using the LongBytesHasher
        IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
        IntegrationTestUtils.pipeInput(
            inputTopic(), NUM_PARTITIONS_INPUT, producer, System::currentTimeMillis, 0, 100L,
            LongStream.range(0, 32).toArray()
        );

        // Then
        IntegrationTestUtils.awaitOutput(
            outputTopic(),
            0,
            LongStream.range(0, 32)
                .boxed()
                .map(k -> new KeyValue<>(k, 100L))
                .collect(Collectors.toSet()),
            false,
            properties
        );
      }

      // have this outside the try block so that kafka streams is closed and fully
      // flushed before we assert
      final String cassandraName = new TableName(storeName).tableName();
      final var partitioner = TablePartitioner.defaultPartitioner();
      final CassandraFactTable table = CassandraFactTable.create(
          new DefaultTableSpec(cassandraName, partitioner, TtlResolver.NO_TTL, config), client);

      final var offset0 = table.lastWrittenOffset(0);
      final var offset1 = table.lastWrittenOffset(1);

      assertThat(offset0, is(notNullValue()));
      assertThat(offset1, is(notNullValue()));

      Assertions.assertEquals(table.lastWrittenOffset(2), NO_COMMITTED_OFFSET);

      LOG.info("Checking data in remote table");
      // these store ValueAndTimestamp, so we need to just pluck the last 8 bytes
      for (long k = 0; k < 32; k++) {
        final var kBytes = Bytes.wrap(serializer.serialize("", k));
        final byte[] valBytes = table.get((int) k % NUM_PARTITIONS_INPUT, kBytes, MIN_VALID_TS);
        final Long val = deserializer.deserialize("foo", Arrays.copyOfRange(valBytes, 8, 16));
        assertThat(val, is(100L));
      }
    }
  }

  // TODO(sophie): finish and re-enable this
  //@Test
  public void shouldFlushToRemoteTableWithSegmentPartitions() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);

    final var params = ResponsiveWindowParams
        .window(storeName, WINDOW_SIZE, GRACE_PERIOD, false)
        .withNumSegments(NUM_SEGMENTS);
    try (
        final var streams = new ResponsiveKafkaStreams(
            windowStoreTopology(params),
            properties
        );
        final var serializer = new LongSerializer();
        final var deserializer = new LongDeserializer()
    ) {
      // When:
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(20), streams);

      // this will send one key to each kafka partition, with 100 records across 10s
      // tumbling windows that count the number of records per window
      // equally across the same 10 active segments for key=0, and across 20 rolling
      // segments for key=1
      final Function<KeyValue<Long, Long>, Long> timestampForValue = kv -> {
        if (kv.key == 0) {
          return kv.value % 10;
        } else if (kv.key == 1) {
          return kv.value % 5;
        } else {
          throw new IllegalStateException("Key should be only 0 or 1");
        }
      };

      IntegrationTestUtils.pipeInput(
          inputTopic(), NUM_PARTITIONS_INPUT, producer, timestampForValue, 0, 100L,
          LongStream.range(0, NUM_PARTITIONS_INPUT).toArray());

      // Then
      IntegrationTestUtils.awaitOutput(
          outputTopic(),
          0,
          LongStream.range(0, 32)
              .boxed()
              .map(k -> new KeyValue<>(k, 100L))
              .collect(Collectors.toSet()),
          false,
          properties
      );
      final String cassandraName = new TableName(storeName).tableName();
      final var partitioner = SubPartitioner.create(
          OptionalInt.empty(),
          NUM_PARTITIONS_INPUT,
          cassandraName,
          ResponsiveConfig.responsiveConfig(properties),
          storeName + "-changelog"
      );
      final CassandraKeyValueTable table = CassandraKeyValueTable.create(
          new DefaultTableSpec(cassandraName, partitioner, TtlResolver.NO_TTL, config), client);

      assertThat(client.numPartitions(cassandraName), is(OptionalInt.of(32)));
      assertThat(client.count(cassandraName, 0), is(2L));
      assertThat(client.count(cassandraName, 16), is(2L));

      // these store ValueAndTimestamp, so we need to just pluck the last 8 bytes
      for (long k = 0; k < 32; k++) {
        final var kBytes = Bytes.wrap(serializer.serialize("", k));
        assertThat(
            deserializer.deserialize(
                "foo",
                Arrays.copyOfRange(
                    table.get(
                        partitioner.tablePartition((int) (k % NUM_PARTITIONS_INPUT), kBytes),
                        kBytes,
                        MIN_VALID_TS),
                    8,
                    16)),
            is(100L));
      }
    }
  }

  private Topology windowStoreTopology(
      final ResponsiveWindowParams params
  ) {
    final var builder = new StreamsBuilder();

    final KStream<Long, Long> stream = builder.stream(inputTopic());
    stream.groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD))
        .count(ResponsiveStores.windowMaterialized(params))
        .toStream()
        .selectKey((k, v) -> k.key())
        .to(outputTopic());

    return builder.build();
  }

  private Topology keyValueStoreTopology(
      final ResponsiveKeyValueParams params
  ) {
    final var builder = new StreamsBuilder();

    final KStream<Long, Long> stream = builder.stream(inputTopic());
    stream.groupByKey()
        .count(ResponsiveStores.materialized(params))
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
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, LongSerde.class.getName());

    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

    properties.put(CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, 32);
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
          .map(k -> k / NUM_PARTITIONS_INPUT) // we want 0 -> 0, 1 -> 16, 2 -> 1, 3 -> 17 ...
          .map(Long::intValue)
          .orElse(null);
    }
  }
}
