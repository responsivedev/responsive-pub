/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.clients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
 *   specified in the {@code assign} call. These records would otherwise be dropped.</li>
 * </ol>
 *
 * <p>This class breaks a lot of abstraction barriers, but allows us to
 * support remote {@link org.apache.kafka.streams.kstream.GlobalKTable}s without
 * forking Kafka Streams.</p>
 */
public class ResponsiveGlobalConsumer extends DelegatingConsumer<byte[], byte[]> {

  private final int defaultApiTimeoutMs;
  private final Admin admin;

  public ResponsiveGlobalConsumer(
      final Map<String, Object> config,
      final Consumer<byte[], byte[]> delegate,
      final Admin admin
  ) {
    super(delegate);
    final ConsumerConfig consumerConfig = new ConsumerConfig(config);
    this.defaultApiTimeoutMs = consumerConfig.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
    this.admin = admin;
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
  public void unsubscribe() {
    // since this consumer has ENABLE_AUTO_COMMIT_CONFIG set to true,
    // it is not guaranteed that any commits will happen before calling
    // unsubscribe - the GlobalStreamThread will call unsubscribe
    // when it's finished restoring, so we should make sure to commit
    // any offsets at this point so that when it resumes normal operation
    // it doesn't experience any time travel
    commitSync();
    super.unsubscribe();
  }

  @Deprecated
  public ConsumerRecords<byte[], byte[]> poll(final long timeoutMs) {
    return poll(Duration.ofMillis(timeoutMs));
  }

  @Override
  public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
    final ConsumerRecords<byte[], byte[]> poll = super.poll(timeout);
    return SingletonConsumerRecords.of(poll);
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
      return super.position(partition, duration);
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
  public void close() {
    super.close();
    admin.close();
  }
  
  @Override
  public void close(final Duration timeout) {
    super.close(timeout);
    admin.close();
  }

  /**
   * A hack that will return all records that were polled when calling
   * {@link #records(TopicPartition)}.
   */
  private static final class SingletonConsumerRecords extends ConsumerRecords<byte[], byte[]> {

    static SingletonConsumerRecords of(final ConsumerRecords<byte[], byte[]> records) {
      Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
      records.partitions().forEach(p -> map.put(p, records.records(p)));
      return new SingletonConsumerRecords(map, records.nextOffsets());
    }

    public SingletonConsumerRecords(
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records,
        final Map<TopicPartition, OffsetAndMetadata> nextOffsets
    ) {
      super(records, nextOffsets);
    }

    @Override
    public List<ConsumerRecord<byte[], byte[]>> records(final TopicPartition partition) {
      final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
      super.records(partition.topic()).forEach(consumerRecords::add);
      return consumerRecords;
    }
  }
}
