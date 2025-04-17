package dev.responsive.kafka.internal.snapshot.topic;

import dev.responsive.kafka.internal.snapshot.Snapshot;
import dev.responsive.kafka.internal.snapshot.SnapshotStore;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSnapshotStore implements SnapshotStore {
  private static final Logger LOG = LoggerFactory.getLogger(TopicSnapshotStore.class);

  private final TopicPartition topicPartition;
  private final Supplier<Producer<SnapshotStoreRecordKey, SnapshotStoreRecord>> producerSupplier;
  private final Supplier<Consumer<SnapshotStoreRecordKey, SnapshotStoreRecord>> consumerSupplier;
  private final Thread readerThread;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicReference<Snapshot> currentSnapshot
      = new AtomicReference<>(Snapshot.initial());
  private final ConcurrentMap<Long, Snapshot> snapshots = new ConcurrentHashMap<>();
  private final SynchronizedConsumerPosition consumedOffset = new SynchronizedConsumerPosition();
  private final Consumer<?, ?> endOffsetConsumer;

  public TopicSnapshotStore(
      final String topic,
      final short replicas,
      final Supplier<Consumer<SnapshotStoreRecordKey, SnapshotStoreRecord>> consumerSupplier,
      final Supplier<Producer<SnapshotStoreRecordKey, SnapshotStoreRecord>> producerSupplier,
      final Admin admin
  ) {
    this.topicPartition = new TopicPartition(topic, 0);
    this.producerSupplier = producerSupplier;
    this.consumerSupplier = consumerSupplier;
    createTopic(admin, replicas);
    final var consumer = consumerSupplier.get();
    consumer.assign(List.of(topicPartition));
    consumer.seekToBeginning(List.of(topicPartition));
    readerThread = new Thread(() -> runReader(
        consumer,
        currentSnapshot,
        snapshots,
        consumedOffset,
        running
    ));
    readerThread.start();
    this.endOffsetConsumer = consumerSupplier.get();
    this.endOffsetConsumer.assign(List.of(topicPartition));
    waitTillConsumedAll();
  }

  public static TopicSnapshotStore create(
      final String topic,
      final short replicas,
      final Map<String, Object> config
  ) {
    final Map<String, Object> consumerConfig = new HashMap<>(config);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, null);
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerConfig.put(
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());
    final Supplier<Consumer<SnapshotStoreRecordKey, SnapshotStoreRecord>> consumerSupplier = () ->
        new KafkaConsumer<>(
            consumerConfig,
            new SnapshotStoreSerdes.SnapshotStoreRecordKeyDeserializer(),
            new SnapshotStoreSerdes.SnapshotStoreRecordDeserializer()
        );
    final Map<String, Object> producerConfig = new HashMap<>(config);
    final String appId = config.get(StreamsConfig.APPLICATION_ID_CONFIG).toString();
    producerConfig.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        String.format("__responsive-%s-snapshot-store", appId)
    );
    final Supplier<Producer<SnapshotStoreRecordKey, SnapshotStoreRecord>> producerSupplier = () ->
        new KafkaProducer<>(
            producerConfig,
            new SnapshotStoreSerdes.SnapshotStoreRecordKeySerializer(),
            new SnapshotStoreSerdes.SnapshotStoreRecordSerializer()
        );
    try (final Admin admin = Admin.create(config)) {
      return new TopicSnapshotStore(
          topic, replicas, consumerSupplier, producerSupplier, admin);
    }
  }

  @Override
  public Snapshot currentSnapshot(boolean block) {
    if (block) {
      waitTillConsumedAll();
    }
    return currentSnapshot.get();
  }

  @Override
  public List<Snapshot> listSnapshots(boolean block) {
    if (block) {
      waitTillConsumedAll();
    }
    return snapshots.values().stream()
        .sorted(Comparator.comparingLong(Snapshot::generation))
        .collect(Collectors.toList());
  }

  @Override
  public Snapshot updateCurrentSnapshot(
      final Function<Snapshot, Snapshot> updater
  ) {
    final Future<RecordMetadata> sendFut;
    final Snapshot updated;
    try (final var producer = producerSupplier.get()) {
      producer.initTransactions();
      producer.beginTransaction();
      try {
        waitTillConsumedAll();
        updated = updater.apply(currentSnapshot.get());
        final var record = createRecord(updated);
        sendFut = producer.send(record);
        producer.commitTransaction();
      } catch (final RuntimeException e) {
        producer.abortTransaction();
        throw e;
      }
    }
    final RecordMetadata recordMetadata;
    try {
      recordMetadata = sendFut.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    waitTillConsumerPosition(recordMetadata.offset() + 1);
    return updated;
  }

  @Override
  public void close() {
    try {
      endOffsetConsumer.close();
    } catch (final RuntimeException e) {
      LOG.warn("error closing end offset consumer", e);
    }
    running.set(false);
    try {
      readerThread.join();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ProducerRecord<SnapshotStoreRecordKey, SnapshotStoreRecord> createRecord(
      final Snapshot snapshot
  ) {
    return new ProducerRecord<>(
        topicPartition.topic(),
        topicPartition.partition(),
        new SnapshotStoreRecordKey(SnapshotStoreRecordType.Snapshot, snapshot.generation()),
        new SnapshotStoreRecord(SnapshotStoreRecordType.Snapshot, snapshot)
    );
  }

  private void createTopic(final Admin admin, final short replicas) {
    try {
      final var result = admin.createTopics(List.of(
          new NewTopic(topicPartition.topic(), 1, replicas)
      ));
      result.all().get();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOG.info("snapshot store topic already exists.");
      } else {
        throw new RuntimeException(e);
      }
    } catch (final TopicExistsException e) {
      LOG.info("snapshot store topic already exists.");
    }
  }

  private void waitTillConsumedAll() {
    waitTillConsumerPosition(endOffset());
  }

  private void waitTillConsumerPosition(final long offset) {
    consumedOffset.waitTillConsumerPosition(offset);
  }

  private long endOffset() {
    synchronized (endOffsetConsumer) {
      return endOffsetConsumer.endOffsets(List.of(topicPartition)).get(topicPartition);
    }
  }

  private void runReader(
      final Consumer<SnapshotStoreRecordKey, SnapshotStoreRecord> consumer,
      final AtomicReference<Snapshot> currentSnapshot,
      final ConcurrentMap<Long, Snapshot> allSnapshots,
      final SynchronizedConsumerPosition consumedOffset,
      final AtomicBoolean running
  ) {
    while (running.get()) {
      final ConsumerRecords<SnapshotStoreRecordKey, SnapshotStoreRecord> records
          = consumer.poll(Duration.ofMillis(100));
      for (final ConsumerRecord<SnapshotStoreRecordKey, SnapshotStoreRecord> record : records) {
        switch (record.key().type()) {
          case Snapshot: {
            final Snapshot update = record.value().snapshot().get();
            currentSnapshot.getAndUpdate(c -> {
              if (update.generation() >= c.generation()) {
                return update;
              } else {
                return c;
              }
            });
            allSnapshots.put(update.generation(), update);
            break;
          }
          default: {
            throw new IllegalStateException();
          }
        }
      }
      consumedOffset.updateConsumerPosition(consumer.position(topicPartition));
    }
  }
}
