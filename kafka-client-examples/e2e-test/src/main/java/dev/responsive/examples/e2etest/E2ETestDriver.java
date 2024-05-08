package dev.responsive.examples.e2etest;

import com.google.common.collect.ImmutableMap;
import dev.responsive.examples.e2etest.Schema.InputRecord;
import dev.responsive.examples.e2etest.Schema.OutputRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class E2ETestDriver {
  private static final Logger LOG = LoggerFactory.getLogger(E2ETestDriver.class);

  private final UrandomGenerator randomGenerator = new UrandomGenerator();
  private final Map<String, Object> properties;
  private final int numKeys;
  private final String inputTopic;
  private final int partitions;
  private final Producer<Long, InputRecord> producer;
  private final Consumer<Long, OutputRecord> consumer;
  private final String outputTopic;
  private final List<Long> keys;
  private final Map<Long, ProduceState> produceState;
  private final Map<Long, ConsumeState> consumeState;
  private int outstanding = 0;
  private final Object produceWait = new Object();
  private final int maxOutstanding;
  private final Long recordsToProcess;
  private int recordsProcessed = 0;
  private volatile boolean keepRunning = true;

  public E2ETestDriver(
      final Map<String, Object> properties,
      final int numKeys,
      final String inputTopic,
      final String outputTopic,
      final int partitions,
      final long recordsToProcess,
      final int maxOutstanding,
      final Duration receivedThreshold
  ) {
    this.properties = Objects.requireNonNull(properties);
    this.numKeys = numKeys;
    this.inputTopic = Objects.requireNonNull(inputTopic);
    this.partitions = partitions;
    this.outputTopic = Objects.requireNonNull(outputTopic);
    this.keys = LongStream.range(0, numKeys)
        .map(v -> randomGenerator.nextLong())
        .boxed()
        .toList();
    produceState = keys.stream().collect(Collectors.toMap(
        k -> k,
        ProduceState::new
    ));
    consumeState = keys.stream().collect(Collectors.toMap(
        k -> k,
        k -> new ConsumeState(k, receivedThreshold)
    ));
    this.recordsToProcess = recordsToProcess;
    this.maxOutstanding = maxOutstanding;
    final Map<String, Object> producerProperties = ImmutableMap.<String, Object>builder()
        .putAll(properties)
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Schema.InputRecordSerializer.class)
        .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .build();
    producer = new KafkaProducer<>(producerProperties);
    final Map<String, Object> consumerProperties = ImmutableMap.<String, Object>builder()
        .putAll(properties)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Schema.OutputRecordDeserializer.class)
        .put(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT))
        .build();
    consumer = new KafkaConsumer<>(consumerProperties);
  }

  public void notifyStop() {
    synchronized (produceWait) {
      keepRunning = false;
      produceWait.notify();
    }
  }

  public void start() {
    E2ETestUtils.retryFor(
        () -> E2ETestUtils.maybeCreateTopics(
            properties, partitions, List.of(inputTopic, outputTopic)),
        Duration.ofMinutes(5)
    );
    final Thread t = new Thread(this::runProducer);
    t.start();
    runConsumer();
    try {
      t.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.printf("processed %d records\n", recordsProcessed);
    System.out.printf("produced by key: %s\n", produceState.values().stream()
        .map(v -> String.format("key(%d) count(%d)\n", v.key, v.sendCount))
        .collect(Collectors.joining(","))
    );
    System.out.printf("processed by key: %s\n", consumeState.values().stream()
        .map(v -> String.format("key(%d) count(%d)", v.key, v.recvdCount))
        .collect(Collectors.joining(","))
    );
  }

  private void runConsumer() {
    final var tps = IntStream.range(0, partitions)
        .mapToObj(p -> new TopicPartition(outputTopic, p))
        .toList();
    consumer.assign(tps);
    consumer.seekToBeginning(tps);
    while (keepRunning) {
      pollOnce();
      if (recordsProcessed >= recordsToProcess) {
        notifyStop();
      }
    }
  }

  private void runProducer() {
    int produced = 0;
    while (true) {
      synchronized (produceWait) {
        outstanding += produced;
        while (outstanding >= maxOutstanding && keepRunning) {
          try {
            produceWait.wait();
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
      if (!keepRunning) {
        break;
      }
      produced = produceNextBatch();
    }
  }

  private int produceNextBatch() {
    final Map<Long, ArrayList<Future<RecordMetadata>>> futures = new HashMap<>();
    int batchSz = 32;
    for (int i = 0; i < batchSz; i++) {
      final var record = nextRecord();
      futures.computeIfAbsent(record.key(), k -> new ArrayList<>());
      final var rmf = producer.send(record);
      futures.get(record.key()).add(rmf);
    }
    producer.flush();
    for (final long k : futures.keySet()) {
      for (final var rmf : futures.get(k)) {
        final RecordMetadata rm;
        try {
          rm = rmf.get();
        } catch (final InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
        produceState.get(k).recordSend(rm.partition(), rm.offset());
      }
    }
    return batchSz;
  }

  private void pollOnce() {
    final var pollDuration = Duration.ofSeconds(10);
    final ConsumerRecords<Long, OutputRecord> consumed = consumer.poll(pollDuration);
    for (final var cr : consumed.records(outputTopic)) {
      final ProduceState ps = produceState.get(cr.key());
      final ConsumeState cs = consumeState.get(cr.key());
      final List<Long> poppedOffsets = ps.popOffsets(cr.value().offset());
      synchronized (produceWait) {
        outstanding -= poppedOffsets.size();
        if (outstanding < maxOutstanding) {
          produceWait.notify();
        }
      }
      recordsProcessed += poppedOffsets.size();
      maybeLogConsumed();
      cs.updateReceived(poppedOffsets, ps.partition(), cr.value().digest());
    }
    for (final var k : consumeState.keySet()) {
      final int outstanding = produceState.get(k).outstandingMessages();
      if (outstanding > 0) {
        consumeState.get(k).checkStalled();
      }
    }
  }

  private Instant lastLog = Instant.EPOCH;

  private void maybeLogConsumed() {
    if (Instant.now().isBefore(lastLog.plusSeconds(60))) {
      return;
    }
    lastLog = Instant.now();
    LOG.info("consumed {} records", recordsProcessed);
    LOG.info("by key: {}",
        consumeState.values().stream()
            .map(v -> v.key + ":" + v.recvdCount)
            .collect(Collectors.joining(","))
    );
  }

  private ProducerRecord<Long, InputRecord> nextRecord() {
    final var key = keys.get((int) (Math.abs(randomGenerator.nextLong()) % keys.size()));
    final var state = produceState.get(key);
    final var data = new InputRecord(key, state.getAndUpdateCount());
    return new ProducerRecord<>(
        inputTopic,
        data.value(),
        data
    );
  }

  private static class ProduceState {
    private final long key;
    private long sendCount = 0;
    private int partition;
    private final List<Long> offsets = new LinkedList<>();

    private ProduceState(final long key) {
      this.key = key;
    }

    private long count() {
      return sendCount;
    }

    private long getAndUpdateCount() {
      sendCount++;
      return sendCount - 1;
    }

    private synchronized void recordSend(final int partition, final long offset) {
      this.partition = partition;
      offsets.add(offset);
      notify();
    }

    private synchronized int partition() {
      return partition;
    }

    private synchronized int outstandingMessages() {
      return offsets.size();
    }

    private synchronized List<Long> popOffsets(final long upTo) {
      while (!offsets.contains(upTo)) {
        try {
          wait();
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      final List<Long> popped = new ArrayList<>();
      while (!offsets.isEmpty() && offsets.get(0) <= upTo) {
        popped.add(offsets.remove(0));
      }
      return popped;
    }
  }

  private static class ConsumeState {
    private final long key;
    private final Duration receivedThreshold;
    private long recvdCount = 0;
    private Instant lastReceived = Instant.now();
    private AccumulatingChecksum checksum = new AccumulatingChecksum();

    private ConsumeState(final long key, final Duration receivedThreshold) {
      this.key = key;
      this.receivedThreshold = receivedThreshold;
    }

    private void updateReceived(
        final List<Long> offsets,
        final int partition,
        final byte[] observedChecksum
    ) {
      lastReceived = Instant.now();
      for (final var o : offsets) {
        checksum = checksum
            .updateWith(recvdCount)
            .updateWith(o)
            .updateWith(partition);
        recvdCount += 1;
      }
      final var expectedChecksum = checksum.current();
      if (!Arrays.equals(expectedChecksum, observedChecksum)) {
        LOG.error("checksum mismatch - key({}), recvdCount({}), {} {}",
            key,
            recvdCount,
            Arrays.toString(checksum.current()),
            observedChecksum
        );
        throw new IllegalStateException("checksum mismatch");
      }
    }

    private void checkStalled() {
      if (Duration.between(lastReceived, Instant.now()).compareTo(receivedThreshold) > 0) {
        throw new IllegalStateException(String.format(
            "have not seen any results for %d in %s",
            key,
            receivedThreshold
        ));
      }
    }
  }
}
