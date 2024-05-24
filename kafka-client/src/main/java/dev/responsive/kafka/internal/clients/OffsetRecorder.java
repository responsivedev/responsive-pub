package dev.responsive.kafka.internal.clients;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record offsets written to kafka and committed to consumer groups. When a commit happens,
 * notify listeners about the set of offsets.
 * <p>
 * The callbacks here need to be synchronized. Almost all the callbacks happen on the stream
 * thread. The lone exception is the Send callback, which happens on Producer I/O threads.
 */
public class OffsetRecorder {
  private static final Logger LOG = LoggerFactory.getLogger(OffsetRecorder.class);

  private final Map<RecordingKey, Long> offsetsToBeCommitted = new HashMap<>();
  private final Map<TopicPartition, Long> offsetsWritten = new HashMap<>();
  private final ProducerListener producerListener = new ProducerListener();
  private final ConsumerListener consumerListener = new ConsumerListener();
  private final List<CommitCallback> commitCallback = new LinkedList<>();
  private final boolean eos;

  public OffsetRecorder(final boolean eos) {
    this.eos = eos;
  }

  public synchronized void addCommitCallback(final CommitCallback callback) {
    commitCallback.add(callback);
  }

  public ResponsiveProducer.Listener getProducerListener() {
    return producerListener;
  }

  public ResponsiveConsumer.Listener getConsumerListener() {
    return consumerListener;
  }

  private synchronized void clearAllOffsets() {
    offsetsWritten.clear();
    offsetsToBeCommitted.clear();
  }

  private synchronized void onConsumedOffsets(
      final String consumerGroup,
      final TopicPartition partition,
      final long offset
  ) {
    final var rk = new RecordingKey(partition, consumerGroup);
    if (offsetsToBeCommitted.containsKey(rk) && offsetsToBeCommitted.get(rk) > offset) {
      LOG.error("attempting to record earlier offset {} than previously observed for commit {}",
          offset,
          offsetsToBeCommitted.get(rk)
      );
      throw new IllegalStateException(String.format(
          "attempting to record earlier offset %d than previously observed for commit %d",
          offset,
          offsetsToBeCommitted.get(rk))
      );
    }
    offsetsToBeCommitted.put(new RecordingKey(partition, consumerGroup), offset);
  }

  private synchronized void onProduce(final RecordMetadata recordMetadata) {
    final TopicPartition partition
        = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
    offsetsWritten.compute(
        partition,
        (k, v) -> v == null ? recordMetadata.offset() : Math.max(recordMetadata.offset(), v)
    );
  }

  private void onCommit() {
    final Map<RecordingKey, Long> committedOffsets;
    final Map<TopicPartition, Long> writtenOffsets;
    synchronized (this) {
      committedOffsets = Map.copyOf(offsetsToBeCommitted);
      writtenOffsets = Map.copyOf(offsetsWritten);
      offsetsToBeCommitted.clear();
      offsetsWritten.clear();
    }
    commitCallback.forEach(c -> c.onCommit(committedOffsets, writtenOffsets));
  }

  private synchronized void onAbort() {
    offsetsToBeCommitted.clear();
    offsetsWritten.clear();
  }

  @FunctionalInterface
  public interface CommitCallback {
    void onCommit(
        final Map<RecordingKey, Long> committedOffsets,
        final Map<TopicPartition, Long> lastWrittenOffsets
    );
  }

  private class ConsumerListener implements ResponsiveConsumer.Listener {
    @Override
    public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (eos) {
        throw new IllegalStateException("consumer commit is not expected with EOS");
      }
      offsets.forEach((p, o) -> onConsumedOffsets("", p, o.offset()));
      OffsetRecorder.this.onCommit();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      clearAllOffsets();
    }

    @Override
    public void onUnsubscribe() {
      clearAllOffsets();
    }
  }

  private class ProducerListener implements ResponsiveProducer.Listener {
    @Override
    public void onSendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           String consumerGroupId) {
      if (!eos) {
        throw new IllegalStateException("producer commit is not expected with alos");
      }
      offsets.forEach((p, o) -> onConsumedOffsets(consumerGroupId, p, o.offset()));
    }

    @Override
    public void onCommit() {
      if (!eos) {
        throw new IllegalStateException("producer commit is not expected with alos");
      }
      OffsetRecorder.this.onCommit();
    }

    @Override
    public void onAbort() {
      OffsetRecorder.this.onAbort();
    }

    @Override
    public void onSendCompleted(final RecordMetadata recordMetadata) {
      onProduce(recordMetadata);
    }
  }

  public static class RecordingKey {
    private final TopicPartition partition;
    private final String consumerGroup;

    public RecordingKey(
        final TopicPartition partition,
        final String consumerGroup
    ) {
      this.partition = partition;
      this.consumerGroup = consumerGroup;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RecordingKey that = (RecordingKey) o;
      return Objects.equals(partition, that.partition)
          && Objects.equals(consumerGroup, that.consumerGroup);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, consumerGroup);
    }

    @Override
    public String toString() {
      return "RecordingKey{" + "partition=" + partition
          + ", consumerGroup='" + consumerGroup + '\'' + '}';
    }

    public TopicPartition getPartition() {
      return partition;
    }

    public String getConsumerGroup() {
      return consumerGroup;
    }
  }
}
