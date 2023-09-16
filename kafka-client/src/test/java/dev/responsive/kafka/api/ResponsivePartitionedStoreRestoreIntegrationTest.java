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

package dev.responsive.kafka.api;

import static dev.responsive.utils.IntegrationTestUtils.getCassandraValidName;
import static dev.responsive.utils.IntegrationTestUtils.pipeInput;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaTypes.KVSchema;
import dev.responsive.utils.IntegrationTestUtils;
import dev.responsive.utils.ResponsiveConfigParam;
import dev.responsive.utils.ResponsiveExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@ExtendWith(ResponsiveExtension.class)
public class ResponsivePartitionedStoreRestoreIntegrationTest {

  private static final int MAX_POLL_MS = 5000;
  private static final String INPUT_TOPIC = "input";
  private static final String INPUT_TBL_TOPIC = "input_tbl";
  private static final String OUTPUT_TOPIC = "output";
  private static final String OUTPUT_JOINED = "output_join";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) throws InterruptedException, ExecutionException {
    this.name = getCassandraValidName(info);
    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    final var result = admin.createTopics(
        List.of(
            new NewTopic(inputTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(inputTblTopic(), Optional.of(1), Optional.empty())
                .configs(Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)),
            new NewTopic(outputTopic(), Optional.of(1), Optional.empty()),
            new NewTopic(outputJoined(), Optional.of(1), Optional.empty())
        )
    );
    result.all().get();
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(inputTopic(), inputTblTopic(), outputTopic(), outputJoined()));
  }

  @ParameterizedTest
  @EnumSource(KVSchema.class)
  public void shouldFlushStoresBeforeClose(final KVSchema type) throws Exception {
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);
    final KafkaClientSupplier defaultClientSupplier = new DefaultKafkaClientSupplier();
    final CassandraClientFactory defaultFactory = new DefaultCassandraClientFactory();
    final TopicPartition input = new TopicPartition(inputTopic(), 0);

    try (final ResponsiveKafkaStreams streams
             = buildAggregatorApp(properties, defaultClientSupplier, defaultFactory, type, false)) {
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(30), streams);
      // Send some data through
      pipeInput(inputTopic(), 1, producer, System::currentTimeMillis, 0, 10, 0);
      // Wait for it to be processed
      waitTillFullyConsumed(input, Duration.ofSeconds(120));

      // Make sure changelog is even w/ cassandra
      final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(properties);
      final CassandraClient cassandraClient = defaultFactory.createCassandraClient(
          defaultFactory.createCqlSession(config),
          config
      );
      final RemoteKeyValueSchema statements =
          cassandraClient.prepareKVTableSchema(params(type, aggName()));
      final long cassandraOffset = statements.metadata(aggName(), 0).offset;
      assertThat(cassandraOffset, greaterThan(0L));
      final TopicPartition changelog
          = new TopicPartition(name + "-" + aggName() + "-changelog", 0);
      final List<ConsumerRecord<Long, Long>> changelogRecords
          = slurpPartition(changelog, properties);
      final long last = changelogRecords.get(changelogRecords.size() - 1).offset();
      assertThat(cassandraOffset, equalTo(last));
    }
  }

  @ParameterizedTest
  @EnumSource(KVSchema.class)
  public void shouldRestoreUnflushedChangelog(final KVSchema type) throws Exception {
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);
    final KafkaClientSupplier defaultClientSupplier = new DefaultKafkaClientSupplier();
    final CassandraClientFactory defaultFactory = new DefaultCassandraClientFactory();
    final TopicPartition inputTbl = new TopicPartition(inputTblTopic(), 0);
    final TopicPartition input = new TopicPartition(inputTopic(), 0);

    try (final ResponsiveKafkaStreams streams
             = buildAggregatorApp(properties, defaultClientSupplier, defaultFactory, type, false)) {
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(10), streams);
      // Send some data through
      pipeInput(inputTblTopic(), 1, producer, System::currentTimeMillis, 0, 10, 0, 1, 2, 3);
      waitTillFullyConsumed(inputTbl, Duration.ofSeconds(120));
      pipeInput(inputTopic(), 1, producer, System::currentTimeMillis, 0, 10, 0);
      // Wait for it to be processed
      waitTillFullyConsumed(input, Duration.ofSeconds(120));
    }

    // restart with fault injecting cassandra client
    final FaultInjectingCassandraClientSupplier cassandraFaultInjector
        = new FaultInjectingCassandraClientSupplier();
    try (final ResponsiveKafkaStreams streams
             = buildAggregatorApp(
                 properties, defaultClientSupplier, cassandraFaultInjector, type, false)) {
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(10), streams);

      // Inject a fault into cassandra client so it fails the next flush
      final Fault fault = new Fault(new RuntimeException("oops"));
      cassandraFaultInjector.fault.set(fault);

      // Send some more data through and wait for it to be committed
      final long endInput = endOffset(input);
      pipeInput(inputTopic(), 1, producer, System::currentTimeMillis, 10, 20, 0);
      producer.flush();
      waitTillConsumedPast(input, endInput + 1, Duration.ofSeconds(30));
    }

    // Make sure changelog is ahead of cassandra
    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(properties);
    final CassandraClient cassandraClient = defaultFactory.createCassandraClient(
        defaultFactory.createCqlSession(config),
        config);
    final RemoteKeyValueSchema statements =
        cassandraClient.prepareKVTableSchema(params(type, aggName()));

    final long cassandraOffset = statements.metadata(aggName(), 0).offset;
    assertThat(cassandraOffset, greaterThan(0L));
    final TopicPartition changelog = new TopicPartition(name + "-" + aggName() + "-changelog", 0);
    final long changelogOffset = admin.listOffsets(Map.of(changelog, OffsetSpec.latest())).all()
        .get()
        .get(changelog)
        .offset();
    assertThat(cassandraOffset, lessThan(changelogOffset));

    // Restart with restore recorder
    final TestKafkaClientSupplier recordingClientSupplier = new TestKafkaClientSupplier();
    try (final ResponsiveKafkaStreams streams
             = buildAggregatorApp(properties, recordingClientSupplier, defaultFactory, type, true)
    ) {
      IntegrationTestUtils.startAppAndAwaitRunning(Duration.ofSeconds(30), streams);
      // Send some more data through and check output
      pipeInput(inputTopic(), 1, producer, System::currentTimeMillis, 20, 30, 0);
      producer.flush();
      waitTillFullyConsumed(input, Duration.ofSeconds(120));
    }

    // Assert that the final aggregation result is correct
    final Optional<ConsumerRecord<Long, Long>> lastRecord = readLastOutputRecord(properties);
    assertThat(lastRecord.isPresent(), is(true));
    assertThat(lastRecord.get().key(), is(0L));
    assertThat(lastRecord.get().value(), is(LongStream.range(0, 30).sum()));
    assertThat(recordingClientSupplier.restoreRecords.keySet(), hasItem(changelog));
    assertThat(recordingClientSupplier.restoreRecords.get(changelog), not(empty()));
    // Assert that we never restored from an offset earlier than committed to Cassandra
    for (final ConsumerRecord<?, ?> r :  recordingClientSupplier.restoreRecords.get(changelog)) {
      assertThat(r.offset(), greaterThanOrEqualTo(cassandraOffset));
    }
    // Assert that our source table is never truncated and the changelog table is
    assertThat(firstOffset(inputTbl), is(0L));
    assertThat(firstOffset(changelog), greaterThan(0L));
  }

  private Optional<ConsumerRecord<Long, Long>> readLastOutputRecord(
      final Map<String, Object> properties
  ) {
    final List<ConsumerRecord<Long, Long>> all
        = slurpPartition(new TopicPartition(outputTopic(), 0), properties);
    return all.size() == 0 ? Optional.empty() : Optional.of(all.get(all.size() - 1));
  }

  private List<ConsumerRecord<Long, Long>> slurpPartition(
      final TopicPartition partition,
      final Map<String, Object> originals
  ) {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(
        ISOLATION_LEVEL_CONFIG,
        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)
    );
    final List<ConsumerRecord<Long, Long>> allRecords = new LinkedList<>();
    try (final KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(properties)) {
      final long end = consumer.endOffsets(List.of(partition)).get(partition);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));

      while (consumer.position(partition) < end) {
        final ConsumerRecords<Long, Long> records = consumer.poll(Duration.ofSeconds(30));
        allRecords.addAll(records.records(partition));
      }
      return allRecords;
    }
  }

  @SuppressWarnings("unchecked")
  private ResponsiveKafkaStreams buildAggregatorApp(
      final Map<String, Object> originals,
      final KafkaClientSupplier clientSupplier,
      final CassandraClientFactory cassandraClientFactory,
      final KVSchema type,
      final boolean truncateChangelog) {
    final Map<String, Object> properties = new HashMap<>(originals);

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<Long, Long> input = builder.stream(inputTopic());
    final String inputTableName = name + "inputTbl";

    final KTable<Long, Long> inputTbl = builder.table(
        inputTblTopic(),
        ResponsiveStores.materialized(
            type == KVSchema.FACT
                ? ResponsiveKeyValueParams.fact(inputTableName)
                : ResponsiveKeyValueParams.keyValue(inputTableName)
        )
    );

    final ResponsiveKeyValueParams baseParams = type == KVSchema.FACT
        ? ResponsiveKeyValueParams.fact(aggName())
        : ResponsiveKeyValueParams.keyValue(aggName());

    input
        .groupByKey()
        .aggregate(
            () -> 0L,
            (k, v, va) -> v + va,
            ResponsiveStores.<Long, Long>materialized(
                truncateChangelog
                    ? baseParams.withTruncateChangelog()
                    : baseParams
            ).withLoggingEnabled(
                Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)))
        .toStream()
        .to(outputTopic());
    input.join(inputTbl, Long::sum);

    final Properties builderProperties = new Properties();
    builderProperties.putAll(properties);
    return new ResponsiveKafkaStreams(
        builder.build(builderProperties),
        ResponsiveConfig.loggedConfig(properties),
        new StreamsConfig(properties),
        clientSupplier,
        cassandraClientFactory
    );
  }

  private String aggName() {
    return name + "agg";
  }

  private String inputTopic() {
    return name + "." + INPUT_TOPIC;
  }

  private String inputTblTopic() {
    return name + "." + INPUT_TBL_TOPIC;
  }

  private String outputTopic() {
    return name + "." + OUTPUT_TOPIC;
  }

  private String outputJoined() {
    return name + "." + OUTPUT_JOINED;
  }

  private static class Fault {
    final RuntimeException exception;

    private Fault(final RuntimeException exception) {
      this.exception = exception;
    }

    private void fire() {
      throw exception;
    }
  }

  private static class FaultInjectingCassandraClientSupplier implements CassandraClientFactory {
    private final CassandraClientFactory wrappedFactory = new DefaultCassandraClientFactory();
    private final AtomicReference<Fault> fault = new AtomicReference<>(null);

    @Override
    public CqlSession createCqlSession(ResponsiveConfig config) {
      final CqlSession wrapped = wrappedFactory.createCqlSession(config);
      final var spy = spy(wrapped);
      doAnswer(a -> {
        final Fault fault = this.fault.get();
        if (fault != null && a.getArgument(0) instanceof BatchStatement) {
          fault.fire();
        }
        return wrapped.execute((Statement<?>) a.getArgument(0));
      }).when(spy).execute(any(Statement.class));
      return spy;
    }

    @Override
    public CassandraClient createCassandraClient(
        final CqlSession session,
        final ResponsiveConfig responsiveConfigs
    ) {
      return wrappedFactory.createCassandraClient(session, responsiveConfigs);
    }
  }

  private static class TestKafkaClientSupplier extends DefaultKafkaClientSupplier {
    private final Map<TopicPartition, Collection<ConsumerRecord<byte[], byte[]>>> restoreRecords
        = new ConcurrentHashMap<>();

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
      return new RestoreRecordRecordingConsumer(config, restoreRecords);
    }
  }

  private static class RestoreRecordRecordingConsumer extends KafkaConsumer<byte[], byte[]> {
    private final Map<TopicPartition, Collection<ConsumerRecord<byte[], byte[]>>> recorded;

    public RestoreRecordRecordingConsumer(
        final Map<String, Object> configs,
        final Map<TopicPartition, Collection<ConsumerRecord<byte[], byte[]>>> recorded
    ) {
      super(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
      this.recorded = recorded;
    }

    @Override
    @SuppressWarnings("deprecation")
    public ConsumerRecords<byte[], byte[]> poll(long timeoutMs) {
      return record(super.poll(timeoutMs));
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
      return record(super.poll(timeout));
    }

    public ConsumerRecords<byte[], byte[]> record(final ConsumerRecords<byte[], byte[]> records) {
      for (final var p : records.partitions()) {
        recorded.computeIfAbsent(p, k -> new ConcurrentLinkedQueue<>()).addAll(records.records(p));
      }
      return records;
    }
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
    properties.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);

    properties.put(COMMIT_INTERVAL_MS_CONFIG, 0);
    properties.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), 20_000);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    properties.put(consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_MS - 1);

    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 0);

    return properties;
  }

  private ResponsiveKeyValueParams params(final KVSchema type, final String name) {
    switch (type) {
      case KEY_VALUE:  return ResponsiveKeyValueParams.keyValue(name);
      case FACT:       return ResponsiveKeyValueParams.fact(name);
      default:         throw new IllegalArgumentException();
    }
  }

  private long endOffset(final TopicPartition topic)
      throws ExecutionException, InterruptedException {
    return admin.listOffsets(Map.of(topic, OffsetSpec.latest())).all().get()
        .get(topic)
        .offset();
  }

  private long firstOffset(final TopicPartition topic)
      throws ExecutionException, InterruptedException {
    return admin.listOffsets(Map.of(topic, OffsetSpec.earliest())).all().get()
        .get(topic)
        .offset();
  }

  private void waitTillFullyConsumed(
      final TopicPartition partition,
      final Duration timeout
  ) throws ExecutionException, InterruptedException, TimeoutException {
    waitTillConsumedPast(partition, endOffset(partition), timeout);
  }

  private void waitTillConsumedPast(
      final TopicPartition partition,
      final long offset,
      final Duration timeout
  ) throws ExecutionException, InterruptedException, TimeoutException {
    final Instant start = Instant.now();
    while (Instant.now().isBefore(start.plus(timeout))) {
      final Map<String, Map<TopicPartition, OffsetAndMetadata>> listing
          = admin.listConsumerGroupOffsets(name).all().get();
      if (listing.get(name).containsKey(partition)) {
        final long committed = listing.get(name).get(partition).offset();
        if (committed >= offset) {
          return;
        }
      }
      Thread.sleep(1000);
    }
    throw new TimeoutException("timed out waiting for app to fully consume input");
  }
}