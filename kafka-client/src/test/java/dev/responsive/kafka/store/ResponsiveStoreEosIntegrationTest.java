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

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
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

import com.datastax.oss.driver.api.core.CqlSession;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.ResponsiveDriver;
import dev.responsive.utils.ContainerExtension;
import dev.responsive.utils.RemoteMonitor;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;

@ExtendWith(ContainerExtension.class)
public class ResponsiveStoreEosIntegrationTest {

  private static final Logger LOG
      = LoggerFactory.getLogger(ResponsiveStoreEosIntegrationTest.class);

  private static final int MAX_POLL_MS = 5000;
  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final String STORE_NAME = "store";

  private String name;
  private String bootstrapServers;
  private Admin admin;
  private CqlSession session;
  private CassandraClient client;
  private ScheduledExecutorService executor;

  @BeforeEach
  public void before(
      final TestInfo info,
      final CassandraContainer<?> cassandra,
      final KafkaContainer kafka
  ) {
    name = info.getTestMethod().orElseThrow().getName();
    session = CqlSession.builder()
        .addContactPoint(cassandra.getContactPoint())
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .withKeyspace("responsive_clients") // NOTE: this keyspace is expected to exist
        .build();
    client = new CassandraClient(session);
    executor = new ScheduledThreadPoolExecutor(2);
    bootstrapServers = kafka.getBootstrapServers();
    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));

    admin.createTopics(
        List.of(
            new NewTopic(INPUT_TOPIC, Optional.of(2), Optional.empty()),
            new NewTopic(OUTPUT_TOPIC, Optional.of(1), Optional.empty())
        )
    );
  }

  @AfterEach
  public void after() {
    session.close();
    executor.shutdown();
    admin.deleteTopics(List.of(INPUT_TOPIC, OUTPUT_TOPIC));
    admin.close();
  }

  @Test
  public void shouldMaintainStateOnEosFailOverAndFenceOldClient() throws Exception {
    // Given:
    final Map<String, Object> properties = getMutableProperties();
    final KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties);
    final SharedState state = new SharedState();

    // When:
    try (
        final KafkaStreams streamsA = buildStreams(properties, "a", state);
        final KafkaStreams streamsB = buildStreams(properties, "b", state);
    ) {
      startAndAwaitRunning(Duration.ofSeconds(10), streamsA, streamsB);

      // committed data first, then second set of uncommitted data
      pipeInput(producer, System::currentTimeMillis, 0, 10, 0, 1);
      pipeInput(producer, System::currentTimeMillis, 10, 15, 0, 1);

      new RemoteMonitor(executor, () -> state.numCommits.get() == 2)
          .await(Duration.ofSeconds(5));

      // 20 values have been committed (10 for each partition) and
      // an additional 10 values are uncommitted (5 for each partition)
      // this statement below just blocks until we've read 30 uncommitted
      // records before causing one of the tasks to stall
      readOutput(0, 30, true, properties);

      state.stall.set(Stall.INJECTED);

      // pipe some more data, when the stalled client times out its
      // topic-partitions should be assigned to the first client, at
      // which point 5 of the writes should be aborted and reprocessed
      // by the new client
      pipeInput(producer, System::currentTimeMillis, 15, 20, 0, 1);

      // the stall only happens on process, so this needs to be after the
      // previous pipeInput call
      new RemoteMonitor(executor, () -> state.stall.get() == Stall.STALLING)
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
      final List<KeyValue<Long, Long>> readC = readOutput(0, 40, false, properties);
      final List<KeyValue<Long, Long>> readU = readOutput(0, 45, true, properties);

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
    final Map<String, Object> properties = new HashMap<>();

    properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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

  private StoreBuilder<KeyValueStore<Long, Long>> storeSupplier() {
    final ResponsiveDriver responsiveDriver = new ResponsiveDriver(session, admin);
    return Stores.keyValueStoreBuilder(
        responsiveDriver.kv(STORE_NAME),
        Serdes.Long(),
        Serdes.Long()
    ).withLoggingEnabled(
        Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE));
  }

  private KafkaStreams buildStreams(
      final Map<String, Object> originals,
      final String instance,
      final SharedState state
  ) {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(APPLICATION_SERVER_CONFIG, instance + ":1024");

    final StreamsConfig config = new StreamsConfig(properties);
    final StreamsBuilder builder = new StreamsBuilder();
    builder.addStateStore(storeSupplier());

    final KStream<Long, Long> input = builder.stream(INPUT_TOPIC);
    input
        .process(() -> new TestProcessor(instance, state), STORE_NAME)
        .to(OUTPUT_TOPIC);

    return new KafkaStreams(builder.build(), config);
  }

  private void startAndAwaitRunning(
      final Duration timeout,
      final KafkaStreams... streams
  ) throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final Condition onRunning = lock.newCondition();
    final Boolean[] running = new Boolean[]{false, false};

    for (int i = 0; i < streams.length; i++) {
      final int idx = i;
      final StateListener oldListener = getStateListener(streams[i]);
      final StateListener listener = (newState, oldState) -> {
        if (oldListener != null) {
          oldListener.onChange(newState, oldState);
        }

        lock.lock();
        try {
          running[idx] = (newState == State.RUNNING);
          onRunning.signalAll();
        } finally {
          lock.unlock();
        }
      };
      streams[i].setStateListener(listener);
    }

    for (final KafkaStreams stream : streams) {
      stream.start();
    }

    final long end = System.nanoTime() + timeout.toNanos();
    lock.lock();
    try {
      while (Arrays.stream(running).anyMatch(b -> !b)) {
        if (System.nanoTime() > end
            || !onRunning.await(end - System.nanoTime(), TimeUnit.NANOSECONDS)) {
          throw new TimeoutException("Not all streams were running after " + timeout);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void pipeInput(
      final KafkaProducer<Long, Long> producer,
      final Supplier<Long> timestamp,
      final long valFrom,
      final long valTo,
      final long... keys
  ) {
    for (final long k : keys) {
      for (long v = valFrom; v < valTo; v++) {
        producer.send(new ProducerRecord<>(
            INPUT_TOPIC,
            (int) k % 2,
            timestamp.get(),
            k,
            v
        ));
      }
    }
  }

  private List<KeyValue<Long, Long>> readOutput(
      final long from,
      final long numEvents,
      final boolean readUncommitted,
      final Map<String, Object> originals
  ) throws TimeoutException {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(ISOLATION_LEVEL_CONFIG, readUncommitted
        ? IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT)
        : IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));

    try (final KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(properties)) {
      final TopicPartition output = new TopicPartition(OUTPUT_TOPIC, 0);
      consumer.assign(List.of(output));
      consumer.seek(output, from);

      final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      final List<KeyValue<Long, Long>> result = new ArrayList<>();
      while (result.size() < numEvents) {
        // this is configured to only poll one record at a time, so we
        // can guarantee we won't accidentally poll more than numEvents
        final ConsumerRecords<Long, Long> polled = consumer.poll(Duration.ofSeconds(30));
        for (ConsumerRecord<Long, Long> rec : polled) {
          result.add(new KeyValue<>(rec.key(), rec.value()));
        }
        if (System.nanoTime() > end) {
          throw new TimeoutException(
              "Timed out trying to read " + numEvents + " events from " + output);
        }
      }
      return result;
    }
  }

  private static StateListener getStateListener(final KafkaStreams streams) {
    try {
      if (streams instanceof KafkaStreamsNamedTopologyWrapper) {
        final Field field = streams.getClass().getSuperclass().getDeclaredField("stateListener");
        field.setAccessible(true);
        return (StateListener) field.get(streams);
      } else {
        final Field field = streams.getClass().getDeclaredField("stateListener");
        field.setAccessible(true);
        return (StateListener) field.get(streams);
      }
    } catch (final IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException("Failed to get StateListener through reflection", e);
    }
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
    private ProcessorContext<Long, Long> context;
    private KeyValueStore<Long, Long> store;

    public TestProcessor(final String instance, final SharedState state) {
      this.instance = instance;
      this.state = state;
    }

    @Override
    public void init(final ProcessorContext<Long, Long> context) {
      this.context = context;
      this.store = context.getStateStore(STORE_NAME);
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