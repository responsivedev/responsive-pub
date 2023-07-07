package dev.responsive.utils;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;

public final class IntegrationTestUtils {

  private IntegrationTestUtils() {
  }

  public static void pipeInput(
      final String topic,
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
            (int) k % 2,
            timestamp.get(),
            k,
            v
        ));
      }
    }
    producer.flush();
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
              "Timed out trying to read " + numEvents + " events from " + output);
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
}
