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

package dev.responsive.kafka.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * The {@code ResponsiveGlobalConsumer} is a proxy {@link KafkaConsumer} that
 * allows the Responsive code to use consumer groups for populating a
 * {@link org.apache.kafka.streams.kstream.GlobalKTable}. It does four things:
 *
 * <ol>
 *   <li>It intercepts calls to {@link #assign(Collection)} and instead translates
 *   them to {@link #subscribe(Collection)}</li>
 *
 *   <li>It ignores calls to {@link #seek(TopicPartition, long)} so that it
 *   can properly use the committed offsets.</li>
 *
 *   <li>It allows looking up {@link #position(TopicPartition)} for partitions
 *   that this consumer does not own by proxying the Admin for those partitions.</li>
 *
 *   <li>It returns {@link ConsumerRecords} from {@link #poll(Duration)} that will
 *   return all records when calling {@link ConsumerRecords#records(TopicPartition)}
 *   instead of only the requested partition. This is necessary because of the way that
 *   the {@link org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl}
 *   handles restore. Specifically, it will loop partition-by-partition and update
 *   the store for those partitions. The issue with this is that if we use {@code subscribe}
 *   in place of {@code assign} we may get events from partitions that were not
 *   specified in the {@code assign} call. These records will be dropped.</li>
 *
 *   <li>It commits before every call to {@link #poll(Duration)} because
 *   otherwise the Streams implementation for global stores will never commit. We
 *   should consider changing this to every {@code N} polls, but this would be
 *   more difficult to test (we could make it configurable).</li>
 * </ol>
 *
 * <p>This class breaks a lot of abstraction barriers, but allows us to
 * support remote {@link org.apache.kafka.streams.kstream.GlobalKTable}s without
 * forking Kafka Streams.</p>
 */
public class ResponsiveGlobalConsumer implements Consumer<byte[], byte[]> {

  private final Consumer<byte[], byte[]> delegate;
  private final Integer defaultApiTimeoutMs;
  private final Admin admin;

  public ResponsiveGlobalConsumer(
      final Map<String, Object> config,
      final Consumer<byte[], byte[]> delegate,
      final Admin admin
  ) {
    final ConsumerConfig consumerConfig = new ConsumerConfig(config);

    this.delegate = delegate;
    this.defaultApiTimeoutMs = consumerConfig.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
    this.admin = admin;
  }

  @Override
  public Set<TopicPartition> assignment() {
    return delegate.assignment();
  }

  @Override
  public Set<String> subscription() {
    return delegate.subscription();
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener listener) {
    delegate.subscribe(topics, listener);
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    delegate.subscribe(topics);
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener listener) {
    delegate.subscribe(pattern, listener);
  }

  @Override
  public void subscribe(final Pattern pattern) {
    delegate.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    delegate.unsubscribe();
  }

  @Override
  public void assign(final Collection<TopicPartition> partitions) {
    subscribe(
        partitions
            .stream()
            .map(TopicPartition::topic)
            .collect(Collectors.toSet())
    );
  }

  @Override
  @Deprecated
  public ConsumerRecords<byte[], byte[]> poll(final long timeoutMs) {
    return poll(Duration.ofMillis(timeoutMs));
  }

  @Override
  public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
    commitSync();
    final ConsumerRecords<byte[], byte[]> poll = delegate.poll(timeout);
    return SingletonConsumerRecords.of(poll);
  }

  @Override
  public void commitSync() {
    delegate.commitSync();
  }

  @Override
  public void commitSync(final Duration timeout) {
    delegate.commitSync(timeout);
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    delegate.commitSync(offsets);
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      final Duration timeout) {
    delegate.commitSync(offsets, timeout);
  }

  @Override
  public void commitAsync() {
    delegate.commitAsync();
  }

  @Override
  public void commitAsync(final OffsetCommitCallback callback) {
    delegate.commitAsync(callback);
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      final OffsetCommitCallback callback) {
    delegate.commitAsync(offsets, callback);
  }

  @Override
  public void seek(final TopicPartition partition, final long offset) {
  }

  @Override
  public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
  }

  @Override
  public void seekToBeginning(final Collection<TopicPartition> partitions) {
  }

  @Override
  public void seekToEnd(final Collection<TopicPartition> partitions) {
  }

  @Override
  public long position(final TopicPartition partition) {
    return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
  }

  @Override
  public long position(final TopicPartition partition, final Duration duration) {
    if (assignment().contains(partition)) {
      return delegate.position(partition, duration);
    }

    // we may not be assigned this partition, in which case someone else
    // has it, and we should check whether they're caught up
    try {
      final OffsetAndMetadata result = admin.listConsumerGroupOffsets(
              groupMetadata().groupId())
          .partitionsToOffsetAndMetadata()
          .get(duration.toMillis(), TimeUnit.MILLISECONDS)
          .get(partition);

      // if the result is null that means the consumer group hasn't been
      // created yet - just return 0 so that we issue the poll
      return result == null ? 0 : result.offset();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } catch (final TimeoutException e) {
      throw new org.apache.kafka.common.errors.TimeoutException(e);
    }
  }

  @Override
  @Deprecated
  public OffsetAndMetadata committed(final TopicPartition partition) {
    return delegate.committed(partition);
  }

  @Override
  @Deprecated
  public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
    return delegate.committed(partition, timeout);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
    return delegate.committed(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
      final Duration timeout) {
    return delegate.committed(partitions, timeout);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return delegate.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    return delegate.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
    return delegate.partitionsFor(topic, timeout);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return delegate.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
    return delegate.listTopics(timeout);
  }

  @Override
  public void pause(final Collection<TopicPartition> partitions) {
    delegate.pause(partitions);
  }

  @Override
  public void resume(final Collection<TopicPartition> partitions) {
    delegate.resume(partitions);
  }

  @Override
  public Set<TopicPartition> paused() {
    return delegate.paused();
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch) {
    return delegate.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      final Map<TopicPartition, Long> timestampsToSearch, final Duration timeout) {
    return delegate.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
    return delegate.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions,
      final Duration timeout) {
    return delegate.beginningOffsets(partitions, timeout);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
    return delegate.endOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions,
      final Duration timeout) {
    return delegate.endOffsets(partitions, timeout);
  }

  @Override
  public OptionalLong currentLag(final TopicPartition topicPartition) {
    return delegate.currentLag(topicPartition);
  }

  @Override
  public ConsumerGroupMetadata groupMetadata() {
    return delegate.groupMetadata();
  }

  @Override
  public void enforceRebalance(final String reason) {
    delegate.enforceRebalance(reason);
  }

  @Override
  public void enforceRebalance() {
    delegate.enforceRebalance();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void close(final Duration timeout) {
    delegate.close(timeout);
  }

  @Override
  public void wakeup() {
    delegate.wakeup();
  }

  /**
   * A hack that will return all records that were polled when calling
   * {@link #records(TopicPartition)}.
   */
  private static final class SingletonConsumerRecords extends ConsumerRecords<byte[], byte[]> {

    static SingletonConsumerRecords of(final ConsumerRecords<byte[], byte[]> records) {
      Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
      records.partitions().forEach(p -> map.put(p, records.records(p)));
      return new SingletonConsumerRecords(map);
    }

    public SingletonConsumerRecords(
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records
    ) {
      super(records);
    }

    @Override
    public List<ConsumerRecord<byte[], byte[]>> records(final TopicPartition partition) {
      final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
      super.records(partition.topic()).forEach(consumerRecords::add);
      return consumerRecords;
    }
  }
}
