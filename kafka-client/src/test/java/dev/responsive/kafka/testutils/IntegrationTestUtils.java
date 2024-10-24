package dev.responsive.kafka.testutils;

import static org.apache.kafka.clients.CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.junit.jupiter.api.TestInfo;

public final class IntegrationTestUtils {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<ValueAndTimestamp<String>> VALUE_AND_TIMESTAMP_STRING_SERDE =
      new ValueAndTimestampSerde<>(STRING_SERDE);

  /**
   * Simple override that allows plugging in a custom CassandraClientFactory
   * to mock or verify this connection in tests
   */
  public static class MockResponsiveKafkaStreams extends ResponsiveKafkaStreams {
    public MockResponsiveKafkaStreams(
        final Topology topology,
        final Map<?, ?> config,
        final KafkaClientSupplier clientSupplier,
        final CassandraClientFactory clientFactory
    ) {
      super(
          new Params(topology, config)
              .withClientSupplier(clientSupplier)
              .withCassandraClientFactory(clientFactory)
              .build()
      );
    }
  }

  public static ResponsiveConfig copyConfigWithOverrides(
      final ResponsiveConfig original,
      final Map<String, Object> overrides
  ) {
    final var configMap = original.originals();
    configMap.putAll(overrides);
    return ResponsiveConfig.responsiveConfig(configMap);
  }

  public static ResponsiveConfig dummyConfig() {
    final Properties props = new Properties();
    props.put(ResponsiveConfig.CASSANDRA_DATACENTER_CONFIG, "responsive");
    props.put(ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "localhost");
    props.put(ResponsiveConfig.CASSANDRA_PORT_CONFIG, 666);
    props.put(ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "responsive");
    props.put(ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "itest");
    return ResponsiveConfig.responsiveConfig(props);
  }

  public static ResponsiveConfig dummyConfig(final Map<?, ?> overrides) {
    final Properties props = new Properties();
    props.put(ResponsiveConfig.CASSANDRA_DATACENTER_CONFIG, "responsive");
    props.put(ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "localhost");
    props.put(ResponsiveConfig.CASSANDRA_PORT_CONFIG, 666);
    props.put(ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "responsive");
    props.put(ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ttd");
    props.putAll(overrides);
    return ResponsiveConfig.responsiveConfig(props);
  }

  public static <D> byte[] serialize(final D data, final Serde<D> serde) {
    return serde.serializer().serialize("ignored", data);
  }

  public static Bytes serializedKey(final String key) {
    return Bytes.wrap(serialize(key, STRING_SERDE));
  }

  public static byte[] serializedValue(final String value) {
    return serialize(value, STRING_SERDE);
  }

  public static byte[] serializedValueAndTimestamp(final String value, final long timestamp) {
    final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(value, timestamp);
    return serialize(valueAndTimestamp, VALUE_AND_TIMESTAMP_STRING_SERDE);
  }

  public static long minutesToMillis(final long minutes) {
    return minutes * 60 * 1000L;
  }

  public static String getCassandraValidName(final TestInfo info) {
    // add displayName to name to account for parameterized tests
    return info.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT)
        + info.getDisplayName().substring("[X] ".length()).toLowerCase(Locale.ROOT)
        .replace(" ", "") // this can happen if multiple params in parameterized test
        .replace(",", "") // this can happen if multiple params in parameterized test
        .replace("_", ""); // keep only valid cassandra chars to keep testing code easier
  }

  /**
   * Create the given topics with the default number of partitions (1) and wait for them
   * to finish being created.
   * <p>
   * To set the number of partitions explicitly, use {@link #createTopicsAndWait(Admin, Map)}
   * or {@link #createTopicsAndWait(Admin, int, String...)}
   */
  public static void createTopicsAndWait(final Admin admin, final String... topicNames) {
    final List<NewTopic> newTopics = new ArrayList<>();
    for (final String topic : topicNames) {
      newTopics.add(new NewTopic(topic, Optional.of(1), Optional.empty()));
    }
    try {
      admin.createTopics(newTopics).all().get();
    } catch (final Exception e) {
      throw new RuntimeException("Topic creation failed", e);
    }
  }

  public static void createTopicsAndWait(
      final Admin admin,
      final int numPartitions,
      final String... topicNames
  ) {
    final List<NewTopic> newTopics = new ArrayList<>();
    for (final String topic : topicNames) {
      newTopics.add(new NewTopic(topic, Optional.of(numPartitions), Optional.empty()));
    }
    try {
      admin.createTopics(newTopics).all().get();
    } catch (final Exception e) {
      throw new RuntimeException("Topic creation failed", e);
    }
  }

  public static void createTopicsAndWait(
      final Admin admin,
      final Map<String, Integer> topicToPartitions
  ) {
    final List<NewTopic> newTopics = new ArrayList<>();
    for (final var entry : topicToPartitions.entrySet()) {
      newTopics.add(new NewTopic(entry.getKey(), Optional.of(entry.getValue()), Optional.empty()));
    }

    try {
      admin.createTopics(newTopics).all().get();
    } catch (final Exception e) {
      throw new RuntimeException("Topic creation failed", e);
    }
  }

  public static void pipeInput(
      final String topic,
      final int partitions,
      final KafkaProducer<Long, Long> producer,
      final Supplier<Long> timestamp,
      final long valFrom,
      final long valTo,
      final long... keys
  ) {
    for (final long k : keys) {
      for (long v = valFrom; v < valTo; v++) {
        producer.send(new ProducerRecord<>(
            topic,
            (int) k % partitions,
            timestamp.get(),
            k,
            v
        ));
      }
    }
    producer.flush();
  }

  public static void pipeInput(
      final String topic,
      final int partitions,
      final KafkaProducer<Long, Long> producer,
      final Function<KeyValue<Long, Long>, Long> timestampForKV,
      final long valFrom,
      final long valTo,
      final long... keys
  ) {
    for (final long k : keys) {
      for (long v = valFrom; v < valTo; v++) {
        producer.send(new ProducerRecord<>(
            topic,
            (int) k % partitions,
            timestampForKV.apply(new KeyValue<>(k, v)),
            k,
            v
        ));
      }
    }
    producer.flush();
  }

  public static <K, V> void pipeRecords(
      final KafkaProducer<K, V> producer,
      final String topic,
      final List<KeyValue<K, V>> records
  ) {
    for (final KeyValue<K, V> record : records) {
      producer.send(new ProducerRecord<>(
          topic,
          record.key,
          record.value
      ));
    }
    producer.flush();
  }

  public static <K, V> void pipeTimestampedRecords(
      final KafkaProducer<K, V> producer,
      final String topic,
      final List<KeyValueTimestamp<K, V>> records
  ) {
    for (final KeyValueTimestamp<K, V> record : records) {
      producer.send(new ProducerRecord<>(
          topic,
          null,
          record.timestamp(),
          record.key(),
          record.value()
      ));
    }
    producer.flush();
  }

  public static <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> slurpPartitions(
      final List<TopicPartition> partitions,
      final Map<String, Object> originals
  ) {
    return partitions.stream().collect(Collectors.toMap(
        p -> p,
        p -> slurpPartition(p, originals)
    ));
  }

  public static <K, V> List<ConsumerRecord<K, V>> slurpPartition(
      final TopicPartition partition,
      final Map<String, Object> originals
  ) {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(
        ISOLATION_LEVEL_CONFIG,
        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT)
    );
    final List<ConsumerRecord<K, V>> allRecords = new LinkedList<>();
    try (final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties)) {
      final long end = consumer.endOffsets(List.of(partition)).get(partition);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));

      while (consumer.position(partition) < end) {
        final ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(30));
        allRecords.addAll(records.records(partition));
      }
      return allRecords;
    }
  }

  public static void awaitOutput(
      final String topic,
      final long from,
      final Set<KeyValue<Long, Long>> expected,
      final boolean readUncommitted,
      final Map<String, Object> originals
  ) throws TimeoutException {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(ISOLATION_LEVEL_CONFIG, readUncommitted
        ? IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT)
        : IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));

    final var allSeen = new HashSet<>();
    final var notYetSeen = new HashSet<>(expected);
    try (final KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(properties)) {
      final TopicPartition output = new TopicPartition(topic, 0);
      consumer.assign(List.of(output));
      consumer.seek(output, from);

      final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!notYetSeen.isEmpty()) {
        final ConsumerRecords<Long, Long> polled = consumer.poll(Duration.ofSeconds(30));
        for (ConsumerRecord<Long, Long> rec : polled) {
          final var kv = new KeyValue<>(rec.key(), rec.value());
          notYetSeen.remove(kv);
          allSeen.add(kv);
        }
        if (System.nanoTime() > end) {
          throw new TimeoutException(
              "Timed out trying to read " + expected + " events from " + output
                  + ".\nNot yet seen: " + notYetSeen + ".\nAll seen: " + allSeen);
        }
      }
    }
  }

  public static <K, V> List<KeyValue<K, V>> readOutput(
      final String topic,
      final long from,
      final long numEvents,
      final boolean readUncommitted,
      final Map<String, Object> originals
  ) throws TimeoutException {
    return readOutput(topic, from, numEvents, 1, readUncommitted, originals);
  }

  public static <K, V> List<KeyValue<K, V>> readOutput(
      final String topic,
      final long from,
      final long numEvents,
      final int numPartitions,
      final boolean readUncommitted,
      final Map<String, Object> originals
  ) throws TimeoutException {
    final Map<String, Object> properties = new HashMap<>(originals);
    properties.put(ISOLATION_LEVEL_CONFIG, readUncommitted
        ? IsolationLevel.READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT)
        : IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));

    final List<TopicPartition> partitions = IntStream.range(0, numPartitions)
        .mapToObj(i -> new TopicPartition(topic, i))
        .collect(Collectors.toList());

    try (final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties)) {
      consumer.assign(partitions);
      partitions.forEach(tp -> consumer.seek(tp, from));

      final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(300);
      final List<KeyValue<K, V>> result = new ArrayList<>();
      while (result.size() < numEvents) {
        // this is configured to only poll one record at a time, so we
        // can guarantee we won't accidentally poll more than numEvents
        final ConsumerRecords<K, V> polled = consumer.poll(Duration.ofSeconds(30));
        for (ConsumerRecord<K, V> rec : polled) {
          result.add(new KeyValue<>(rec.key(), rec.value()));
        }
        if (System.nanoTime() > end) {
          throw new TimeoutException(
              "Timed out trying to read " + numEvents + " events from " + partitions
                  + ". Read " + result);
        }
      }
      return result;
    }
  }

  public static void startAppAndAwaitRunning(
      final Duration timeout,
      final ResponsiveKafkaStreams... streams
  ) throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final Condition onRunning = lock.newCondition();
    final AtomicBoolean[] running = new AtomicBoolean[streams.length];

    for (int i = 0; i < streams.length; i++) {
      running[i] = new AtomicBoolean(false);
      final int idx = i;
      final StateListener oldListener = streams[i].stateListener();
      final StateListener listener = (newState, oldState) -> {
        if (oldListener != null) {
          oldListener.onChange(newState, oldState);
        }

        lock.lock();
        try {
          running[idx].set(newState == State.RUNNING);
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
      while (Arrays.stream(running).anyMatch(b -> !b.get())) {
        if (System.nanoTime() > end
            || !onRunning.await(end - System.nanoTime(), TimeUnit.NANOSECONDS)) {
          throw new TimeoutException("Not all streams were running after " + timeout);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public static void startAppAndAwaitState(
      final KafkaStreams.State state,
      final Duration timeout,
      final ResponsiveKafkaStreams... streams
  ) throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final Condition onState = lock.newCondition();
    final AtomicBoolean[] inState = new AtomicBoolean[streams.length];

    for (int i = 0; i < streams.length; i++) {
      inState[i] = new AtomicBoolean(false);
      final int idx = i;
      final StateListener oldListener = streams[i].stateListener();
      final StateListener listener = (newState, oldState) -> {
        if (oldListener != null) {
          oldListener.onChange(newState, oldState);
        }

        lock.lock();
        try {
          inState[idx].set(newState == state);
          onState.signalAll();
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
      while (Arrays.stream(inState).anyMatch(b -> !b.get())) {
        if (System.nanoTime() > end
            || !onState.await(end - System.nanoTime(), TimeUnit.NANOSECONDS)) {
          throw new TimeoutException("Not all streams were in " + state + " after " + timeout);
        }
      }
    } finally {
      lock.unlock();
    }
  }


  public static Map<String, Object> getDefaultMutablePropertiesWithStringSerdes(
      final Map<String, Object> responsiveProps,
      final String name
  ) {
    final Map<String, Object> properties = new HashMap<>(responsiveProps);
    properties.put(APPLICATION_ID_CONFIG, name);
    properties.put(NUM_STREAM_THREADS_CONFIG, 1);
    properties.put(COMMIT_INTERVAL_MS_CONFIG, 1); // commit as often as possible

    properties.put(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 5_000 - 1);
    properties.put(consumerPrefix(MAX_POLL_RECORDS_CONFIG), 1);
    properties.put(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG, 1);

    properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    return properties;
  }

  public static long endOffset(final Admin admin, final TopicPartition topic)
      throws ExecutionException, InterruptedException {
    return admin.listOffsets(Map.of(topic, OffsetSpec.latest())).all().get()
        .get(topic)
        .offset();
  }

  public static void waitTillFullyConsumed(
      final Admin admin,
      final TopicPartition partition,
      final String consumerGroupName,
      final Duration timeout
  ) throws ExecutionException, InterruptedException, TimeoutException {
    waitTillConsumedPast(admin, partition, consumerGroupName, endOffset(admin, partition), timeout);
  }

  public static void waitTillConsumedPast(
      final Admin admin,
      final TopicPartition partition,
      final String consumerGroupName,
      final long offset,
      final Duration timeout
  ) throws ExecutionException, InterruptedException, TimeoutException {
    final Instant start = Instant.now();
    while (Instant.now().isBefore(start.plus(timeout))) {
      final Map<String, Map<TopicPartition, OffsetAndMetadata>> listing
          = admin.listConsumerGroupOffsets(consumerGroupName).all().get();
      if (listing.get(consumerGroupName).containsKey(partition)) {
        final long committed = listing.get(consumerGroupName).get(partition).offset();
        if (committed >= offset) {
          return;
        }
      }
      Thread.sleep(1000);
    }
    throw new TimeoutException("timed out waiting for app to fully consume input");
  }

  private IntegrationTestUtils() {
  }

}
