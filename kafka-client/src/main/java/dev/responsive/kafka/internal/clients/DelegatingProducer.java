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
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;

public abstract class DelegatingProducer<K, V> implements Producer<K, V> {

  private final Producer<K, V> delegate;

  public DelegatingProducer(final Producer<K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initTransactions() {
    delegate.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    delegate.beginTransaction();
  }

  @Override
  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final ConsumerGroupMetadata groupMetadata
  ) throws ProducerFencedException {
    delegate.sendOffsetsToTransaction(offsets, groupMetadata);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    delegate.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    delegate.abortTransaction();
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record) {
    return delegate.send(record);
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
    return delegate.send(record, callback);
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(final String topic) {
    return delegate.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return delegate.metrics();
  }

  @Override
  public Uuid clientInstanceId(final Duration duration) {
    return delegate.clientInstanceId(duration);
  }

  @Override
  public void registerMetricForSubscription(final KafkaMetric metric) {
    delegate.registerMetricForSubscription(metric);
  }

  @Override
  public void unregisterMetricFromSubscription(final KafkaMetric metric) {
    delegate.unregisterMetricFromSubscription(metric);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void close(final Duration timeout) {
    delegate.close();
  }

}
