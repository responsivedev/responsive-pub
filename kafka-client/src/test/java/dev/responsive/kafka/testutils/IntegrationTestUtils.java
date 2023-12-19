package dev.responsive.kafka.testutils;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.TestInfo;

public final class IntegrationTestUtils {

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
    props.put(ResponsiveConfig.STORAGE_DATACENTER_CONFIG, "responsive");
    props.put(ResponsiveConfig.STORAGE_HOSTNAME_CONFIG, "localhost");
    props.put(ResponsiveConfig.STORAGE_PORT_CONFIG, 666);
    props.put(ResponsiveConfig.TENANT_ID_CONFIG, "responsive-test");
    return ResponsiveConfig.responsiveConfig(props);
  }

  public static ResponsiveConfig dummyConfig(final Map<?, ?> overrides) {
    final Properties props = new Properties();
    props.put(ResponsiveConfig.STORAGE_DATACENTER_CONFIG, "responsive");
    props.put(ResponsiveConfig.STORAGE_HOSTNAME_CONFIG, "localhost");
    props.put(ResponsiveConfig.STORAGE_PORT_CONFIG, 666);
    props.put(ResponsiveConfig.TENANT_ID_CONFIG, "TTD");
    props.putAll(overrides);
    return ResponsiveConfig.responsiveConfig(props);
  }

  public static String getCassandraValidName(final TestInfo info) {
    // add displayName to name to account for parameterized tests
    return info.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT)
        + info.getDisplayName().substring("[X] ".length()).toLowerCase(Locale.ROOT)
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
      final List<KeyValueTimestamp<K, V>> records
  ) {
    for (final KeyValueTimestamp<K, V> record : records) {
      producer.send(new ProducerRecord<>(
          topic,
          0,
          record.timestamp(),
          record.key(),
          record.value()
      ));
    }
    producer.flush();
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

  public static List<KeyValue<Long, Long>> readOutput(
      final String topic,
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
      final TopicPartition output = new TopicPartition(topic, 0);
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
              "Timed out trying to read " + numEvents + " events from " + output
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

  private IntegrationTestUtils() {
  }

}
