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

package org.apache.kafka.streams.processor.internals;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.clients.ResponsiveGlobalConsumer;
import dev.responsive.utils.ResponsiveExtension;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread.StateListener;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@ExtendWith(ResponsiveExtension.class)
public class GlobalStreamThreadIntegrationTest {

  private static final String GLOBAL_TOPIC_SUFFIX = "global-topic";
  private static final int MAX_POLL = 3;

  private String name;
  private String bootstrapServers;
  private String globalTopic;
  private Admin admin;
  private KafkaProducer<byte[], byte[]> producer;

  private File tempDirA;
  private File tempDirB;
  private Map<String, Object> config;

  @BeforeEach
  public void before(
      final TestInfo info,
      final KafkaContainer kafka,
      @TempDir final File tempDirA,
      @TempDir final File tempDirB)
      throws Exception {
    name = info.getTestMethod().orElseThrow().getName();
    bootstrapServers = kafka.getBootstrapServers();
    admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));

    this.tempDirA = tempDirA;
    this.tempDirB = tempDirB;

    globalTopic = name + "-" + GLOBAL_TOPIC_SUFFIX;
    admin.createTopics(List.of(new NewTopic(globalTopic, Optional.of(2), Optional.empty())));

    int created = 0;
    long end = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < end) {
      created = admin.describeTopics(List.of(globalTopic)).allTopicNames().get().size();
      if (created == 1) {
        break;
      }
    }
    if (created == 0) {
      throw new TimeoutException();
    }

    config =
        Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.GROUP_ID_CONFIG,
            name + "-global",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            MAX_POLL,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            100, // make tests faster
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class);

    producer = new KafkaProducer<>(config);
  }

  @AfterEach
  public void after() {
    admin.deleteTopics(List.of(globalTopic));
    admin.close();
    producer.close();
  }

  @Test
  public void shouldRestoreWithSharedPartitionsAcrossApps() throws Exception {
    // Given:
    // start by producing a few events to the global table to test restore
    // use a numRecords that's not a multiple of MAX_POLL because there's
    // an off-by-one bug in the streams code for restore that will consider
    // restoration complete if we've restored all but the final record
    final int numRecords = MAX_POLL * 4 + 1;
    for (int i = 0; i < numRecords; i++) {
      producer.send(
          new ProducerRecord<>(
              globalTopic, null, new byte[] {'k', (byte) i}, new byte[] {'v', (byte) i}));
    }
    producer.flush();

    // we use this latch to make sure that both consumers have successfully
    // polled a single batch (and therefore subscribe has them evenly shared)
    // before continuing to future batches - this will ensure interleaving
    // instead of one consumer going through the entire backlog
    final CountDownLatch restoredBatch = new CountDownLatch(2);
    final CountDownLatch finishedRestore = new CountDownLatch(4);
    final StateRestoreListener restoreListener =
        new LatchRestoreListener(restoredBatch, finishedRestore);

    // start the stream thread
    final TestStoreSupplier storeSupplier = new TestStoreSupplier();
    final GlobalStreamThread streamThreadA = getThread(storeSupplier, restoreListener, tempDirA);
    final GlobalStreamThread streamThreadB = getThread(storeSupplier, restoreListener, tempDirB);

    final CountDownLatch isRunning = new CountDownLatch(2);
    final StateListener listener =
        (thread, newState, oldState) -> {
          if (newState == GlobalStreamThread.State.RUNNING) {
            isRunning.countDown();
          }
        };

    streamThreadA.setStateListener(listener);
    streamThreadB.setStateListener(listener);

    // we need to use an Executor here because start is overridden in GlobalStreamThread
    // to be a blocking operation! how sad :( - the exec will just kick off each thread
    // the threads themselves will be closed at the end of this test independently of
    // this executor
    ExecutorService exec = Executors.newFixedThreadPool(2);
    exec.submit(streamThreadA::start);
    exec.submit(streamThreadB::start);

    // When:
    if (!finishedRestore.await(30, TimeUnit.SECONDS)) {
      throw new TimeoutException();
    }

    // Then:
    assertThat(storeSupplier.store.approximateNumEntries(), is((long) numRecords));

    isRunning.await(); // make sure we don't get false PENDING_SHUTDOWN transition
    exec.shutdown();
    streamThreadA.shutdown();
    streamThreadB.shutdown();
  }

  @Test
  public void shouldShareWorkInSteadyState() throws Exception {
    // ensure each consumer gets assigned during restore
    final CountDownLatch restoredBatch = new CountDownLatch(2);
    final CountDownLatch finishedRestore = new CountDownLatch(2);
    final StateRestoreListener restoreListener =
        new LatchRestoreListener(restoredBatch, finishedRestore);

    // flushHappened ensures that both have called `poll` and therefore
    // have data assigned to them - otherwise we may not test that the
    // sharing actually happens!
    final TestStoreSupplier storeSupplier = new TestStoreSupplier();
    final GlobalStreamThread streamThreadA = getThread(storeSupplier, restoreListener, tempDirA);
    final GlobalStreamThread streamThreadB = getThread(storeSupplier, restoreListener, tempDirB);

    final int numRecords = MAX_POLL * 6;
    for (int i = 0; i < numRecords / 2; i++) {
      producer.send(
          new ProducerRecord<>(
              globalTopic, null, new byte[] {'k', (byte) i}, new byte[] {'v', (byte) i}));
    }
    producer.flush();

    // start the stream thread
    ExecutorService exec = Executors.newFixedThreadPool(2);
    exec.submit(streamThreadA::start);
    exec.submit(streamThreadB::start);
    if (!finishedRestore.await(30, TimeUnit.SECONDS)) {
      throw new TimeoutException();
    }

    // When:
    for (int i = numRecords / 2; i < numRecords; i++) {
      producer.send(
          new ProducerRecord<>(
              globalTopic, null, new byte[] {'k', (byte) i}, new byte[] {'v', (byte) i}));
    }
    producer.flush();

    // Then:
    long end = System.currentTimeMillis() + 30_000;
    long seen = storeSupplier.store.approximateNumEntries();
    while (seen < numRecords && System.currentTimeMillis() < end) {
      seen = storeSupplier.store.approximateNumEntries();
      Thread.sleep(500); // just busy wait for now
    }
    assertThat(seen, is((long) numRecords));

    exec.shutdown();
    streamThreadA.shutdown();
    streamThreadB.shutdown();
  }

  private GlobalStreamThread getThread(
      final TestStoreSupplier storeSupplier,
      final StateRestoreListener restoreListener,
      final File tempDir) {
    final Time time = new SystemTime();
    final InternalTopologyBuilder builder = new InternalTopologyBuilder();
    builder.addGlobalStore(
        new KeyValueStoreBuilder<>(storeSupplier, new ByteArraySerde(), new ByteArraySerde(), time)
            .withLoggingDisabled(),
        "global",
        null,
        null,
        null,
        globalTopic,
        "global-processor",
        () ->
            new ContextualProcessor<Object, Object, Void, Void>() {
              private KeyValueStore<Object, Object> global;

              @Override
              public void init(final ProcessorContext<Void, Void> context) {
                global = context.getStateStore("global");
              }

              @Override
              public void process(final Record<Object, Object> record) {
                global.put(record.key(), record.value());
              }
            });

    final String baseDirectoryName = tempDir.getAbsolutePath();
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, name + "testAppId");
    properties.put(StreamsConfig.STATE_DIR_CONFIG, baseDirectoryName);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class.getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class.getName());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5_000); // ensure flushes happen
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000); // partial poll return quickly

    final Consumer<byte[], byte[]> globalConsumer =
        new ResponsiveGlobalConsumer(
            this.config,
            new KafkaConsumer<>(
                this.config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
            admin);

    final StreamsConfig config = new StreamsConfig(properties);
    final int i = new Random().nextInt(100000);
    return new GlobalStreamThread(
        builder.rewriteTopology(config).buildGlobalStateTopology(),
        config,
        globalConsumer,
        new StateDirectory(config, time, true, false),
        0,
        new StreamsMetricsImpl(new Metrics(), name + "-client", StreamsConfig.METRICS_LATEST, time),
        time,
        name + "-global-thread" + i,
        restoreListener,
        ignored -> {});
  }

  private static class TestStoreSupplier implements KeyValueBytesStoreSupplier {

    // this is thread safe
    final KeyValueStore<Bytes, byte[]> store = new InMemoryKeyValueStore("global");

    @Override
    public String name() {
      return "global";
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
      return store;
    }

    @Override
    public String metricsScope() {
      return "test";
    }
  }

  private static class LatchRestoreListener implements StateRestoreListener {

    private final CountDownLatch restoredBatch;
    private final CountDownLatch finishedRestore;

    public LatchRestoreListener(
        final CountDownLatch restoredFirstBatchLatch, final CountDownLatch finishedRestore) {
      this.restoredBatch = restoredFirstBatchLatch;
      this.finishedRestore = finishedRestore;
    }

    @Override
    public void onRestoreStart(
        final TopicPartition topicPartition,
        final String storeName,
        final long startingOffset,
        final long endingOffset) {}

    @Override
    public void onBatchRestored(
        final TopicPartition topicPartition,
        final String storeName,
        final long batchEndOffset,
        final long numRestored) {
      restoredBatch.countDown();
      try {
        restoredBatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onRestoreEnd(
        final TopicPartition topicPartition, final String storeName, final long totalRestored) {
      finishedRestore.countDown();
    }
  }
}
