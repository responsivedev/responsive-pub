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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveConsumer<K, V> implements Consumer<K, V> {
  private final Consumer<K, V> wrapped;
  private final List<Listener> listeners;
  private final Logger logger;

  private class RebalanceListener implements ConsumerRebalanceListener {
    final Optional<ConsumerRebalanceListener> wrappedRebalanceListener;

    public RebalanceListener() {
      this(Optional.empty());
    }

    public RebalanceListener(final ConsumerRebalanceListener wrappedRebalanceListener) {
      this(Optional.of(wrappedRebalanceListener));
    }

    private RebalanceListener(final Optional<ConsumerRebalanceListener> wrappedRebalanceListener) {
      this.wrappedRebalanceListener = wrappedRebalanceListener;
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsLost(partitions));
      for (final var l : listeners) {
        ignoreException(() -> l.onPartitionsLost(partitions));
      }
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsRevoked(partitions));
      for (final var l : listeners) {
        ignoreException(() -> l.onPartitionsRevoked(partitions));
      }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsAssigned(partitions));
      for (final var l : listeners) {
        ignoreException(() -> l.onPartitionsAssigned(partitions));
      }
    }
  }

  public ResponsiveConsumer(
      final String clientId,
      final Consumer<K, V> wrapped,
      final List<Listener> listeners
  ) {
    this.logger = LoggerFactory.getLogger(
        ResponsiveConsumer.class.getName() + "." + Objects.requireNonNull(clientId));
    this.wrapped = Objects.requireNonNull(wrapped);
    this.listeners = Objects.requireNonNull(listeners);
  }

  @Override
  public void assign(final Collection<TopicPartition> partitions) {
    wrapped.assign(partitions);
  }

  @Override
  public Set<TopicPartition> assignment() {
    return wrapped.assignment();
  }

  @Override
  public Set<String> subscription() {
    return wrapped.subscription();
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    wrapped.subscribe(topics, new RebalanceListener());
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
    wrapped.subscribe(topics, new RebalanceListener(callback));
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
    wrapped.subscribe(pattern, new RebalanceListener(callback));
  }

  @Override
  public void subscribe(final Pattern pattern) {
    wrapped.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    wrapped.unsubscribe();
  }

  @Override
  @SuppressWarnings("deprecation")
  public ConsumerRecords<K, V> poll(final long timeout) {
    return wrapped.poll(timeout);
  }

  @Override
  public ConsumerRecords<K, V> poll(final Duration timeout) {
    return wrapped.poll(timeout);
  }

  @Override
  public void commitSync() {
    wrapped.commitSync();
  }

  @Override
  public void commitSync(final Duration timeout) {
    wrapped.commitSync(timeout);
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    wrapped.commitSync(offsets);
  }

  @Override
  public void commitSync(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final Duration timeout
  ) {
    wrapped.commitSync(offsets, timeout);
  }

  @Override
  public void commitAsync() {
    wrapped.commitAsync();
  }

  @Override
  public void commitAsync(final OffsetCommitCallback callback) {
    wrapped.commitAsync(callback);
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                          final OffsetCommitCallback callback) {
    wrapped.commitAsync(offsets, callback);
  }

  @Override
  public void seek(final TopicPartition partition, final long offset) {
    wrapped.seek(partition, offset);
  }

  @Override
  public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
    wrapped.seek(partition, offsetAndMetadata);
  }

  @Override
  public void seekToBeginning(final Collection<TopicPartition> partitions) {
    wrapped.seekToBeginning(partitions);
  }

  @Override
  public void seekToEnd(final Collection<TopicPartition> partitions) {
    wrapped.seekToEnd(partitions);
  }

  @Override
  public long position(final TopicPartition partition) {
    return wrapped.position(partition);
  }

  @Override
  public long position(final TopicPartition partition, final Duration timeout) {
    return wrapped.position(partition, timeout);
  }

  @Override
  @SuppressWarnings("deprecation")
  public OffsetAndMetadata committed(final TopicPartition partition) {
    return wrapped.committed(partition);
  }

  @Override
  @SuppressWarnings("deprecation")
  public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
    return wrapped.committed(partition, timeout);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
    return wrapped.committed(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                          final Duration timeout) {
    return wrapped.committed(partitions, timeout);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return wrapped.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    return wrapped.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
    return wrapped.partitionsFor(topic, timeout);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return wrapped.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
    return wrapped.listTopics();
  }

  @Override
  public Set<TopicPartition> paused() {
    return wrapped.paused();
  }

  @Override
  public void pause(final Collection<TopicPartition> partitions) {
    wrapped.pause(partitions);
  }

  @Override
  public void resume(final Collection<TopicPartition> partitions) {
    wrapped.resume(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch
  ) {
    return wrapped.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch,
      final Duration timeout
  ) {
    return wrapped.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
    return wrapped.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions,
                                                    final Duration timeout) {
    return wrapped.beginningOffsets(partitions, timeout);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
    return wrapped.endOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions,
                                              final Duration timeout) {
    return wrapped.endOffsets(partitions, timeout);
  }

  @Override
  public OptionalLong currentLag(final TopicPartition topicPartition) {
    return wrapped.currentLag(topicPartition);
  }

  @Override
  public ConsumerGroupMetadata groupMetadata() {
    return wrapped.groupMetadata();
  }

  @Override
  public void enforceRebalance() {
    wrapped.enforceRebalance();
  }

  @Override
  public void enforceRebalance(final String reason) {
    wrapped.enforceRebalance(reason);
  }

  @Override
  public void close() {
    wrapped.close();
    listeners.forEach(l -> ignoreException(l::onClose));
  }

  @Override
  public void close(final Duration timeout) {
    wrapped.close(timeout);
    listeners.forEach(l -> ignoreException(l::onClose));
  }

  @Override
  public void wakeup() {
    wrapped.wakeup();
  }

  private void ignoreException(final Runnable r) {
    try {
      r.run();
    } catch (final Throwable t) {
      logger.error("error calling rebalance listener", t);
    }
  }

  interface Listener {
    default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsLost(Collection<TopicPartition> partitions) {
    }

    default void onClose() {
    }
  }
}
