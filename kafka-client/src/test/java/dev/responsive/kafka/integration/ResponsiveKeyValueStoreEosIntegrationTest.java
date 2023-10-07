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

import static dev.responsive.kafka.testutils.IntegrationTestUtils.createTopicsAndWait;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.getCassandraValidName;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.pipeInput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.readOutput;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.startAppAndAwaitRunning;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveStores;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.utils.RemoteMonitor;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ResponsiveExtension.class)
public class ResponsiveKeyValueStoreEosIntegrationTest {

  private static final Logger LOG
      = LoggerFactory.getLogger(ResponsiveKeyValueStoreEosIntegrationTest.class);

  private static final int MAX_POLL_MS = 5000;
  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";

  private final Map<String, Object> responsiveProps = new HashMap<>();

  private String name;
  private Admin admin;
  private ScheduledExecutorService executor;

  @BeforeEach
  public void before(
      final TestInfo info,
      final Admin admin,
      @ResponsiveConfigParam final Map<String, Object> responsiveProps
  ) {
    // add displayName to name to account for parameterized tests
    name = getCassandraValidName(info);
    executor = new ScheduledThreadPoolExecutor(2);

    this.responsiveProps.putAll(responsiveProps);

    this.admin = admin;
    createTopicsAndWait(admin, Map.of(inputTopic(), 2, outputTopic(), 1));
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

  @ParameterizedTest
  @EnumSource(KVSchema.class)
  public void shouldMaintainStateOnEosFailOverAndFenceOldClient(final KVSchema type)
      throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);
    final SharedState state = new SharedState();

    // When:
    try (
        final ResponsiveKafkaStreams streamsA = buildStreams(properties, "a", state, type);
        final ResponsiveKafkaStreams streamsB = buildStreams(properties, "b", state, type);
    ) {
      startAppAndAwaitRunning(Duration.ofSeconds(10), streamsA, streamsB);

      // committed data first, then second set of uncommitted data
      pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 0, 10, 0, 1);
      pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 10, 15, 0, 1);

      new RemoteMonitor(executor, () -> state.numCommits.get() == 2, Duration.ofMillis(100))
          .await(Duration.ofSeconds(5));

      // 20 values have been committed (10 for each partition) and
      // an additional 10 values are uncommitted (5 for each partition)
      // this statement below just blocks until we've read 30 uncommitted
      // records before causing one of the tasks to stall
      readOutput(outputTopic(), 0, 30, true, properties);

      state.stall.set(Stall.INJECTED);

      // pipe some more data, when the stalled client times out its
      // topic-partitions should be assigned to the first client, at
      // which point 5 of the writes should be aborted and reprocessed
      // by the new client
      pipeInput(inputTopic(), 2, producer, System::currentTimeMillis, 15, 20, 0, 1);

      // the stall only happens on process, so this needs to be after the
      // previous pipeInput call
      new RemoteMonitor(executor, () -> state.stall.get() == Stall.STALLING, Duration.ofMillis(100))
          .await(Duration.ofSeconds(20));

      // wait for a rebalance
      new RemoteMonitor(executor,
          // if either streams has 2 topic partitions assigned to it
          // then a rebalance has happened
          () -> streamsA.metadataForAllStreamsClients()
              .stream().anyMatch(sm -> sm.topicPartitions().size() == 2)
              || streamsB.metadataForAllStreamsClients()
              .stream().anyMatch(sm -> sm.topicPartitions().size() == 2))
          .await(Duration.ofSeconds(30));

      // 20 more values beyond the first read are now committed (10
      // for each partition), but 5 should never have been committed
      // so there should be more uncommitted reads in total
      final List<KeyValue<Long, Long>> readC = readOutput(outputTopic(), 0, 40, false, properties);
      final List<KeyValue<Long, Long>> readU = readOutput(outputTopic(), 0, 45, true, properties);

      // now we release the stalled consumer
      state.stall.set(Stall.RELEASED);

      // Then:
      // ensure that we were able to process and sum up all the values to the
      // correct result (max) and ensure that the values are always increasing
      // because we're summing positive values - if the values decrease that
      // would indicate we read uncommitted values
      final long[] max = new long[]{0, 0};
      final long[] prev = new long[]{-1L, -1L};
      for (final KeyValue<Long, Long> kv : readC) {
        max[kv.key.intValue()] = Math.max(kv.value, max[kv.key.intValue()]);

        assertThat(prev[kv.key.intValue()], lessThan(kv.value));
        prev[kv.key.intValue()] = kv.value;
      }
      assertThat(max, equalTo(new long[]{190, 190}));

      // for the uncommitted values, we are asserting that the failure
      // which happened at offset 10-15 caused duplicate uncommitted writes
      // to the output topic
      final Map<KeyValue<Long, Long>, Integer> dupes = readU
          .stream()
          .collect(Collectors.toMap(kv -> kv, kv -> 1, Integer::sum));
      for (final long val : new long[]{55, 66, 78, 91, 105}) {
        assertThat(dupes.get(new KeyValue<>(0L, val)), is(2));
      }
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

    // this ensures we can control the commits by explicitly requesting a commit
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 20_000);
    properties.put(producerPrefix(TRANSACTION_TIMEOUT_CONFIG), 20_000);

    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
    properties.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    properties.put(consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), MAX_POLL_MS);
    properties.put(consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), MAX_POLL_MS - 1);

    return properties;
  }

  private StoreBuilder<KeyValueStore<Long, Long>> storeSupplier(KVSchema type) {
    return ResponsiveStores.keyValueStoreBuilder(
        ResponsiveStores.keyValueStore(
            type == KVSchema.FACT
                ? ResponsiveKeyValueParams.fact(name)
                : ResponsiveKeyValueParams.keyValue(name)
            ),
        Serdes.Long(),
        Serdes.Long()
    ).withLoggingEnabled(
        Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE));
  }

  private ResponsiveKafkaStreams buildStreams(
      final Map<String, Object> originals,
      final String instance,
      final SharedState state,
      final KVSchema type
  ) {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(APPLICATION_SERVER_CONFIG, instance + ":1024");

    final StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(storeSupplier(type));

    final KStream<Long, Long> input = builder.stream(inputTopic());
    input
        .process(() -> new TestProcessor(instance, state, name), name)
        .to(outputTopic());

    return new ResponsiveKafkaStreams(builder.build(), properties);
  }

  private static class SharedState {
    // ensures that only one instantiation of the TestProcessor gets stalled
    private final AtomicReference<Stall> stall = new AtomicReference<>(Stall.INIT);
    private final AtomicBoolean shouldError = new AtomicBoolean(false);
    private final AtomicInteger numCommits = new AtomicInteger(0);
  }

  private enum Stall {
    INIT,
    INJECTED,
    STALLING,
    RELEASED
  }

  private static class TestProcessor implements Processor<Long, Long, Long, Long> {

    private final String instance;
    private final SharedState state;
    private final String name;
    private ProcessorContext<Long, Long> context;
    private KeyValueStore<Long, Long> store;

    public TestProcessor(final String instance, final SharedState state, String name) {
      this.instance = instance;
      this.state = state;
      this.name = name;
    }

    @Override
    public void init(final ProcessorContext<Long, Long> context) {
      this.context = context;
      this.store = context.getStateStore(name);
    }

    @Override
    public void process(final Record<Long, Long> record) {
      // only stall the processor of partition 0
      if (record.key() % 2 == 0 && state.stall.compareAndSet(Stall.INJECTED, Stall.STALLING)) {
        stall();
        LOG.info("Instance {} recovered from a stall as a zombie.", instance);
      }

      // commit manually every 10 records
      if ((record.value() + 1) % 10 == 0) {
        context.commit();
        state.numCommits.incrementAndGet();
      }

      // update the state store before failing to ensure
      // proper restoration
      final long sum = updateSum(record.key(), record.value());

      if (state.shouldError.compareAndSet(true, false)) {
        store.put(record.key(), -1L);
        throw new RuntimeException("Injected error and corrupted state!");
      }

      context.forward(new Record<>(record.key(), sum, System.currentTimeMillis()));
    }

    private long updateSum(final long key, final long value) {
      Long sum = store.get(key);
      sum = (sum == null) ? value : sum + value;
      store.put(key, sum);
      return sum;
    }

    @SuppressWarnings("BusyWait")
    private void stall() {
      LOG.info("Instance {} is stalling", instance);
      final StreamThread t = (StreamThread) Thread.currentThread();
      while (state.stall.get() == Stall.STALLING) {
        if (!t.isRunning() || t.isInterrupted()) {
          throw new RuntimeException("Detected interruption");
        }

        try {
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
