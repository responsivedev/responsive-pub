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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class OriginEventTracker
    implements ResponsiveConsumer.Listener, ResponsiveProducer.Listener {

  static final String ORIGIN_EVENT_HEADER_KEY = "_roe";
  static final byte[] ORIGIN_EVENT_MARK = new byte[] {0x1};

  private final OriginEventReporter reporter;
  private final boolean eos;

  @GuardedBy("this")
  private final Map<TopicPartition, OffsetTracker> trackedOffsets = new HashMap<>();
  @GuardedBy("this")
  private final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();

  public OriginEventTracker(
      final OriginEventReporter reporter,
      final boolean eos
  ) {
    this.reporter = reporter;
    this.eos = eos;
  }

  @Override
  public <K, V> ConsumerRecords<K, V> onPoll(final ConsumerRecords<K, V> records) {
    for (final ConsumerRecord<K, V> record : records) {
      final var header = record.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY);
      if (header == null) {
        synchronized (this) {
          final var tracker = trackedOffsets.computeIfAbsent(
              new TopicPartition(record.topic(), record.partition()),
              tp -> new OffsetTracker(record.offset())
          );
          tracker.mark(record.offset());
        }
      }
    }
    return records;
  }

  @Override
  public <K, V> ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
    final var header = record.headers().lastHeader(ORIGIN_EVENT_HEADER_KEY);
    if (header == null) {
      record.headers().add(ORIGIN_EVENT_HEADER_KEY, ORIGIN_EVENT_MARK);
    }
    return record;
  }

  @Override
  public void onConsumerCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (eos) {
      throw new IllegalStateException("Should not use consumer commit on EOS");
    }

    synchronized (this) {
      nextCommitOffsets.putAll(offsets);
    }
    onCommit();
  }

  @Override
  public void onProducerCommit() {
    if (!eos) {
      throw new IllegalStateException("Should not use producer commit on ALOS");
    }

    onCommit();
  }

  @Override
  public synchronized void onSendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final String consumerGroupId
  ) {
    nextCommitOffsets.putAll(offsets);
  }

  private void onCommit() {
    synchronized (this) {
      for (final var tpOffset : nextCommitOffsets.entrySet()) {
        final var tracker = trackedOffsets.get(tpOffset.getKey());
        if (tracker != null) {
          final int count = tracker.countAndShift(tpOffset.getValue().offset());
          reporter.report(tpOffset.getKey(), count);
        }
      }

      nextCommitOffsets.clear();
    }
  }

  @Override
  public synchronized void onPartitionsLost(final Collection<TopicPartition> partitions) {
    // since we won't get commits for these anymore, we should clear the offsets
    // we have seen so far but not committed in case we ever get the topic partition
    // reassigned back to ourselves
    partitions.forEach(trackedOffsets::remove);
  }

  @Override
  public synchronized void onUnsubscribe() {
    // clear all seen tracked offsets that have not yet been committed
    trackedOffsets.clear();
  }

}
