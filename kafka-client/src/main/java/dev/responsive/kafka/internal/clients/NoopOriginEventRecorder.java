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

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class NoopOriginEventRecorder implements OriginEventRecorder {
  @Override
  public <K, V> ConsumerRecords<K, V> onPoll(final ConsumerRecords<K, V> records) {
    return records;
  }

  @Override
  public <K, V> ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
    return record;
  }

  @Override
  public void onConsumerCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void onProducerCommit() {
  }

  @Override
  public void onSendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final String consumerGroupId
  ) {
  }

  @Override
  public void onPartitionsLost(final Collection<TopicPartition> partitions) {
  }

  @Override
  public void onUnsubscribe() {
  }
}
