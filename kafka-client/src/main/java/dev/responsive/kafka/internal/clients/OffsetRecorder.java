package dev.responsive.kafka.internal.clients;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Record offsets written to kafka and committed to consumer groups. When a commit happens,
 * notify listeners about the set of offsets.
 * <p>
 * The callbacks here need to be synchronized. Almost all the callbacks happen on the stream
 * thread. The lone exception is the Send callback, which happens on Producer I/O threads.
 */
public class OffsetRecorder {
  private final Map<RecordingKey, Long> uncommitted = new HashMap<>();
  private final Map<TopicPartition, Long> written = new HashMap<>();
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

  private synchronized void onConsumedOffsets(
      final String consumerGroup,
      final TopicPartition partition,
      final long offset
  ) {
    uncommitted.put(new RecordingKey(partition, consumerGroup), offset);
  }

  private synchronized void onProduce(final RecordMetadata recordMetadata) {
    final TopicPartition partition
        = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
    written.compute(
        partition,
        (k, v) -> v == null ? recordMetadata.offset() : Math.max(recordMetadata.offset(), v)
    );
  }

  private void onCommit() {
    final Map<RecordingKey, Long> committedOffsets;
    final Map<TopicPartition, Long> writtenOffsets;
    synchronized (this) {
      committedOffsets = Map.copyOf(uncommitted);
      writtenOffsets = Map.copyOf(written);
      uncommitted.clear();
      written.clear();
    }
    commitCallback.forEach(c -> c.onCommit(committedOffsets, writtenOffsets));
  }

  private synchronized void onAbort() {
    uncommitted.clear();
    written.clear();
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
