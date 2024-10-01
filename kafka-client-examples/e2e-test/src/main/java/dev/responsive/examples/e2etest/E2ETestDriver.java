package dev.responsive.examples.e2etest;

import com.antithesis.sdk.Lifecycle;
import com.google.common.collect.ImmutableMap;
import dev.responsive.examples.common.E2ETestUtils;
import dev.responsive.examples.common.EventSignals;
import dev.responsive.examples.e2etest.E2ESchema.InputRecord;
import dev.responsive.examples.e2etest.E2ESchema.OutputRecord;
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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
  private final Admin admin;
  private final List<CommittedAndEndOffsets> committedAndEndOffsets = new LinkedList<>();
  private final List<CommittedAndEndOffsets> drainedCommittedAndEndOffsets = new LinkedList<>();
  private final Map<Integer, Long> consumedOutputOffsets = new HashMap<>();
  private final Map<Long, ProduceState> produceState;
  private final Map<Long, ConsumeState> consumeState;
  private int outstanding = 0;
  private final Object produceWait = new Object();
  private final int maxOutstanding;
  private final Long recordsToProcess;
  private int recordsProcessed = 0;
  private boolean setupCompleteSignalFired = false;
  private final String groupId;
  private volatile boolean keepRunning = true;
  private final Map<Integer, StalledPartition> stalledPartitions = new HashMap<>();
  private final Duration faultStopThreshold;
  private final Duration stalledPartitionThreshold;
  private final FaultStopper faultStopper = new FaultStopper();

  public E2ETestDriver(
      final Map<String, Object> properties,
      final int numKeys,
      final String inputTopic,
      final String outputTopic,
      final int partitions,
      final long recordsToProcess,
      final int maxOutstanding,
      final Duration stalledPartitionThreshold,
      final Duration faultStopThreshold,
      final String groupId
  ) {
    this.properties = Objects.requireNonNull(properties);
    this.numKeys = numKeys;
    this.inputTopic = Objects.requireNonNull(inputTopic);
    this.partitions = partitions;
    this.outputTopic = Objects.requireNonNull(outputTopic);
    this.groupId = Objects.requireNonNull(groupId);
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
        ConsumeState::new
    ));
    this.recordsToProcess = recordsToProcess;
    this.maxOutstanding = maxOutstanding;
    admin = Admin.create(properties);
    final Map<String, Object> producerProperties = ImmutableMap.<String, Object>builder()
        .putAll(properties)
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, E2ESchema.InputRecordSerializer.class)
        .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000)
        .put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 240000)
        .build();
    producer = new KafkaProducer<>(producerProperties);
    final Map<String, Object> consumerProperties = ImmutableMap.<String, Object>builder()
        .putAll(properties)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
        .put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            E2ESchema.OutputRecordDeserializer.class
        )
        .put(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT))
        .build();
    consumer = new KafkaConsumer<>(consumerProperties);
    this.faultStopThreshold = Objects.requireNonNull(faultStopThreshold);
    this.stalledPartitionThreshold = Objects.requireNonNull(stalledPartitionThreshold);
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
    LOG.info("created topics");
    final Thread t = new Thread(this::runProducer);
    t.start();
    runConsumer();
    try {
      t.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    checkCommittedAndEndOffsets();
    LOG.info("processed {} records", recordsProcessed);
    LOG.info("produced by key: {}", produceState.values().stream()
        .map(v -> String.format("key(%d) count(%d)\n", v.key, v.sendCount))
        .collect(Collectors.joining(","))
    );
    LOG.info("processed by key: {}", consumeState.values().stream()
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
      maybeCheckCommittedAndEndOffsets();
    }
  }

  private void runProducer() {
    int produced = 0;
    int totalProduced = 0;
    while (true) {
      synchronized (produceWait) {
        outstanding += produced;
        if (outstanding > maxOutstanding) {
          // wait for the produced records to drain
          while (outstanding > 0 && keepRunning) {
            try {
              produceWait.wait();
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
      if (!keepRunning) {
        break;
      }
      produced = produceNextBatch();
      if (totalProduced == 0) {
        LOG.info("produced first batch of {} records", produced);
      }
      totalProduced += produced;
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
        produceState.get(k).recordSend(rm);
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
      final List<RecordMetadata> poppedOffsets = ps.popOffsets(cr.value().offset());
      synchronized (produceWait) {
        outstanding -= poppedOffsets.size();
        if (outstanding < maxOutstanding) {
          produceWait.notify();
        }
      }
      consumedOutputOffsets.put(cr.partition(), cr.offset());
      recordsProcessed += poppedOffsets.size();
      maybeFireSetupCompleteSignal();
      maybeLogConsumed();
      maybeLogAllConsumed();
      cs.updateReceived(poppedOffsets, ps.partition(), cr.value().digest());
    }
  }

  private void maybeFireSetupCompleteSignal() {
    if (!setupCompleteSignalFired && recordsProcessed > 0) {
      LOG.info("Received at least one output record, setup is complete");
      Lifecycle.setupComplete(null);
    }
  }

  private Instant lastAllLog = Instant.EPOCH;
  private Instant lastLog = Instant.EPOCH;

  private void maybeLogAllConsumed() {
    if (Instant.now().isBefore(lastAllLog.plusSeconds(60))) {
      return;
    }
    if (consumeState.values().stream().map(v -> v.recvdCount).allMatch(count -> count > 0)) {
      lastAllLog = Instant.now();
      LOG.info("received at least one of all records: {}", recordsProcessed);
    }
  }

  private void maybeLogConsumed() {
    if (Instant.now().isBefore(lastLog.plusSeconds(60))) {
      return;
    }
    lastLog = Instant.now();

    EventSignals.logNumConsumedOutputRecords(recordsProcessed);
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
    private Instant lastSent = Instant.now();
    private final List<RecordMetadata> sent = new LinkedList<>();

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

    private synchronized void recordSend(final RecordMetadata rm) {
      this.lastSent = Instant.now();
      this.partition = rm.partition();
      sent.add(rm);
      notify();
    }

    private synchronized int partition() {
      return partition;
    }

    private synchronized int outstandingMessages() {
      return sent.size();
    }

    private synchronized RecordMetadata earliestSent() {
      return sent.isEmpty() ? null : sent.get(0);
    }

    private synchronized List<RecordMetadata> popOffsets(final long upTo) {
      final Instant start = Instant.now();
      while (sent.stream().noneMatch(rm -> rm.offset() == upTo)) {
        try {
          wait(30_000);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (Duration.between(start, Instant.now()).getSeconds() > 300) {
          LOG.error("ANTITHESIS NEVER: waited longer than 300 seconds for offset {} {} {}",
              upTo,
              partition,
              sent.isEmpty() ? "null" : sent.get(0).toString()
          );
          throw new IllegalStateException(String.format(
              "waited longer than 300 seconds for offset %d %d %s",
              upTo,
              partition,
              sent.isEmpty() ? "null" : sent.get(0).toString()
          ));
        }
      }
      final List<RecordMetadata> popped = new ArrayList<>();
      while (!sent.isEmpty() && sent.get(0).offset() <= upTo) {
        popped.add(sent.remove(0));
      }
      LOG.warn("removing multiple offsets: {}",
          String.join(", ", popped.stream().map(Object::toString).toList()));
      return popped;
    }
  }

  private Instant lastCommittedOffsetLog = Instant.EPOCH;

  private void maybeCheckCommittedAndEndOffsets() {
    final var now = Instant.now();
    if (now.isBefore(lastCommittedOffsetLog.plus(Duration.ofSeconds(10)))) {
      return;
    }
    lastCommittedOffsetLog = Instant.now();
    checkCommittedAndEndOffsets();
  }

  private void checkCommittedAndEndOffsets() {
    final var offsets = committedAndEndOffsets();
    committedAndEndOffsets.add(offsets);
    checkNoOrphanedKeys();
    checkNoStalledPartitions(offsets);
  }

  private void checkNoStalledPartitions(final CommittedAndEndOffsets offsets) {
    for (int p = 0; p < partitions; p++) {
      final var tp = new TopicPartition(inputTopic, p);
      if (stalledPartitions.containsKey(p)) {
        final var stalledPartition = stalledPartitions.get(p);
        if (offsets.inputCommitted().get(tp) > stalledPartition.offset()) {
          LOG.info("resume faults");
          faultStopper.startFaults();
          stalledPartitions.remove(p);
        } else if (offsets.timestamp()
            .isAfter(stalledPartition.detected().plus(stalledPartitionThreshold))) {
          LOG.error("ANTITHESIS NEVER: Partition {} stalled at {} ({}) since {}",
              p,
              stalledPartition.offset(),
              offsets.inputCommitted.get(tp),
              Duration.between(stalledPartition.detected(), offsets.timestamp())
          );
          throw new IllegalStateException(String.format(
              "Partition %d has not made progress from offset %d (current %d) for %s",
              p,
              stalledPartition.offset(),
              offsets.inputCommitted.get(tp),
              Duration.between(stalledPartition.detected(), offsets.timestamp())
          ));
        }
      } else {
        final List<CommittedAndEndOffsets> allCommittedAndEndOffsets = new LinkedList<>();
        allCommittedAndEndOffsets.addAll(drainedCommittedAndEndOffsets);
        allCommittedAndEndOffsets.addAll(committedAndEndOffsets);
        for (final var os : allCommittedAndEndOffsets) {
          final long currentCommitted = offsets.inputCommitted().get(tp);
          final long committed = os.inputCommitted().get(tp);
          final long end = os.inputEnd().get(tp);
          if (end <= committed) {
            break;
          }
          if (committed < currentCommitted) {
            break;
          }
          if (Duration.between(os.timestamp(), offsets.timestamp())
              .compareTo(faultStopThreshold) > 0) {
            LOG.info("pausing faults due stall on partition {} at {} {}",
                p,
                currentCommitted,
                offsets.timestamp
            );
            faultStopper.stopFaults();
            stalledPartitions.put(p, new StalledPartition(offsets.timestamp(), currentCommitted));
            break;
          }
        }
      }
    }
  }

  private void checkNoOrphanedKeys() {
    // go through the committed+end samples and pull ones for which we've
    // consumed past those outputs
    while (!committedAndEndOffsets.isEmpty()) {
      final var offsets = committedAndEndOffsets.get(0);
      boolean popEntry = true;
      for (final var e : offsets.outputEnd.entrySet()) {
        if (!consumedOutputOffsets.containsKey(e.getKey().partition())) {
          // we haven't consumed any outputs for this partition
          popEntry = false;
          break;
        }
        if (consumedOutputOffsets.get(e.getKey().partition()) < e.getValue()) {
          // we haven't consumed up to the output end offset yet
          popEntry = false;
          break;
        }
      }
      if (!popEntry) {
        break;
      }
      // We have consumed past all the end output offsets for this sample. Check
      // that we didn't skip any key.
      drainedCommittedAndEndOffsets.add(committedAndEndOffsets.remove(0));
      for (final var pse : produceState.entrySet()) {
        final long k = pse.getKey();
        final var ps = pse.getValue();
        final var rm = ps.earliestSent();
        if (rm == null) {
          continue;
        }
        if (offsets.inputCommitted.get(new TopicPartition(inputTopic, rm.partition()))
            > rm.offset()) {
          // we skipped past some key
          throw new IllegalStateException(String.format(
              "Left behind key %d on partition %d offset %d",
              k,
              rm.partition(),
              rm.offset()
          ));
        }
      }
    }
  }

  private CommittedAndEndOffsets committedAndEndOffsets() {
    final List<TopicPartition> inputTopicPartitions = IntStream.range(0, partitions)
        .mapToObj(p -> new TopicPartition(inputTopic, p))
        .toList();
    final var futures = admin.listConsumerGroupOffsets(
        Map.of(
            groupId, new ListConsumerGroupOffsetsSpec().topicPartitions(inputTopicPartitions)
        )
    );
    final Map<TopicPartition, OffsetAndMetadata> committedOffsetAndMet;
    try {
      committedOffsetAndMet = futures.all().get().getOrDefault(groupId, Map.of());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    final var end = consumer.endOffsets(inputTopicPartitions);

    final List<TopicPartition> outputTopicPartitions = IntStream.range(0, partitions)
        .mapToObj(p -> new TopicPartition(outputTopic, p))
        .toList();
    final var outputEnd = consumer.endOffsets(outputTopicPartitions);

    final Map<TopicPartition, Long> committed = new HashMap<>();
    final List<String> descriptions = new LinkedList<>();
    for (final var tp : inputTopicPartitions) {
      final long co
          = (committedOffsetAndMet.containsKey(tp) && committedOffsetAndMet.get(tp) != null)
              ? committedOffsetAndMet.get(tp).offset() : -1;
      committed.put(tp, co);
      descriptions.add(String.format("%s: end(%d) committed(%d)",
          tp,
          end.getOrDefault(tp, 0L),
          co
      ));
    }
    LOG.info("committed/end offsets: {}", String.join(" | ", descriptions));
    return new CommittedAndEndOffsets(Instant.now(), committed, end, outputEnd);
  }

  private record StalledPartition(Instant detected, long offset) {
  }

  private record CommittedAndEndOffsets(
      Instant timestamp,
      Map<TopicPartition, Long> inputCommitted,
      Map<TopicPartition, Long> inputEnd,
      Map<TopicPartition, Long> outputEnd
  ) {
  }

  private static class ConsumeState {
    private final long key;
    private long recvdCount = 0;
    private Instant lastReceived = Instant.now();
    private AccumulatingChecksum checksum = new AccumulatingChecksum();

    private ConsumeState(final long key) {
      this.key = key;
    }

    private void updateReceived(
        final List<RecordMetadata> offsets,
        final int partition,
        final byte[] observedChecksum
    ) {
      lastReceived = Instant.now();
      for (final var rm : offsets) {
        checksum = checksum
            .updateWith(recvdCount)
            .updateWith(rm.offset())
            .updateWith(partition);
        recvdCount += 1;
      }
      final var expectedChecksum = checksum.current();
      if (!Arrays.equals(expectedChecksum, observedChecksum)) {
        LOG.error("ANTITHESIS NEVER: checksum mismatch - key({}), recvdCount({}), {} {}",
            key,
            recvdCount,
            Arrays.toString(checksum.current()),
            observedChecksum
        );
        throw new IllegalStateException("checksum mismatch");
      }
    }
  }

  private static class FaultStopper {
    int refs = 0;
    private Instant stoppedAt = null;

    private Instant stopFaults() {
      if (refs == 0) {
        LOG.info("ANTITHESIS: Stop faults");
        stoppedAt = Instant.now();
      }
      refs += 1;
      return stoppedAt;
    }

    private void startFaults() {
      refs -= 1;
      if (refs == 0) {
        LOG.info("ANTITHESIS: Start faults");
      }
    }
  }
}
