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

package dev.responsive.kafka.clients;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveProducer<K, V> implements Producer<K, V> {
  private final Producer<K, V> wrapped;
  private final OffsetRecorder offsetRecorder = new OffsetRecorder();
  private final List<Listener> listeners;
  private final Logger logger;

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

  private static class OffsetRecorder {
    final Map<RecordingKey, Long> uncommitted = new HashMap<>();

    private void record(
        final String consumerGroup,
        final TopicPartition partition,
        final long offset
    ) {
      uncommitted.put(new RecordingKey(partition, consumerGroup), offset);
    }

    private Map<RecordingKey, Long> onCommit() {
      final Map<RecordingKey, Long> ret = Map.copyOf(uncommitted);
      uncommitted.clear();
      return ret;
    }

    private void onAbort() {
      uncommitted.clear();
    }
  }

  public interface Listener {
    default void onCommit(final Map<RecordingKey, Long> committedOffsets) {
    }

    default void onClose() {
    }
  }

  public ResponsiveProducer(
      final String clientid,
      final Producer<K, V> wrapped,
      final List<Listener> listeners
  ) {
    this.logger = LoggerFactory.getLogger(
        ResponsiveProducer.class.getName() + "." + Objects.requireNonNull(clientid));
    this.wrapped = Objects.requireNonNull(wrapped);
    this.listeners = Objects.requireNonNull(listeners);
  }

  @Override
  public void initTransactions() {
    wrapped.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    wrapped.beginTransaction();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final String consumerGroupId
  ) throws ProducerFencedException {
    offsets.forEach((p, o) -> offsetRecorder.record(consumerGroupId, p, o.offset()));
    wrapped.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  @Override
  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final ConsumerGroupMetadata groupMetadata
  ) throws ProducerFencedException {
    offsets.forEach((p, o) -> offsetRecorder.record(groupMetadata.groupId(), p, o.offset()));
    wrapped.sendOffsetsToTransaction(offsets, groupMetadata);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    wrapped.commitTransaction();
    final Map<RecordingKey, Long> offsets = offsetRecorder.onCommit();
    for (final var listener : listeners) {
      try {
        listener.onCommit(offsets);
      } catch (final Throwable t) {
        logger.error("error from responsive producer commit listener", t);
      }
    }
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    wrapped.abortTransaction();
    offsetRecorder.onAbort();
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record) {
    return wrapped.send(record);
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
    return wrapped.send(record, callback);
  }

  @Override
  public void flush() {
    wrapped.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    return wrapped.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return wrapped.metrics();
  }

  @Override
  public void close() {
    wrapped.close();
    closeListeners();
  }

  @Override
  public void close(final Duration timeout) {
    wrapped.close();
    closeListeners();
  }

  private void closeListeners() {
    for (final var l : listeners) {
      try {
        l.onClose();
      } catch (final Throwable t) {
        logger.error("error during producer listener close", t);
      }
    }
  }
}
