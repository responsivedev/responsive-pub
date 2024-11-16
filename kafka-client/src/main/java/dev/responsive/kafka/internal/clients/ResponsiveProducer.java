/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.clients;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveProducer<K, V> implements Producer<K, V> {
  private final Producer<K, V> wrapped;
  private final List<Listener> listeners;
  private final Logger logger;

  public interface Listener {
    default void onCommit() {
    }

    default void onAbort() {
    }

    default void onSendCompleted(final RecordMetadata recordMetadata) {
    }

    default void onSendOffsetsToTransaction(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final String consumerGroupId
    ) {
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
    wrapped.sendOffsetsToTransaction(offsets, consumerGroupId);
    for (final var l : listeners) {
      l.onSendOffsetsToTransaction(offsets, consumerGroupId);
    }
  }

  @Override
  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final ConsumerGroupMetadata groupMetadata
  ) throws ProducerFencedException {
    wrapped.sendOffsetsToTransaction(offsets, groupMetadata);
    for (final var l : listeners) {
      l.onSendOffsetsToTransaction(offsets, groupMetadata.groupId());
    }
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    wrapped.commitTransaction();
    listeners.forEach(Listener::onCommit);
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    wrapped.abortTransaction();
    listeners.forEach(Listener::onAbort);
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record) {
    return new RecordingFuture(wrapped.send(record), listeners);
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
    return new RecordingFuture(
        wrapped.send(record, new RecordingCallback(callback, listeners)), listeners
    );
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
  public Uuid clientInstanceId(final Duration duration) {
    return wrapped.clientInstanceId(duration);
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
    // TODO(rohan): use consistent error behaviour on all callbacks - just throw up
    for (final var l : listeners) {
      try {
        l.onClose();
      } catch (final Throwable t) {
        logger.error("error during producer listener close", t);
      }
    }
  }

  private static class RecordingFuture implements Future<RecordMetadata> {
    private final Future<RecordMetadata> wrapped;
    private final List<Listener> listeners;

    public RecordingFuture(final Future<RecordMetadata> wrapped, final List<Listener> listeners) {
      this.wrapped = wrapped;
      this.listeners = listeners;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
      return wrapped.isDone();
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
      final RecordMetadata recordMetadata = wrapped.get();
      for (final var l : listeners) {
        l.onSendCompleted(recordMetadata);
      }
      return recordMetadata;
    }

    @Override
    public RecordMetadata get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      final RecordMetadata recordMetadata = wrapped.get(timeout, unit);
      for (final var l : listeners) {
        l.onSendCompleted(recordMetadata);
      }
      return recordMetadata;
    }
  }

  private static class RecordingCallback implements Callback {
    private final Callback wrapped;
    private final List<Listener> listeners;

    public RecordingCallback(final Callback wrapped, final List<Listener> listeners) {
      this.wrapped = wrapped;
      this.listeners = listeners;
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
      wrapped.onCompletion(metadata, exception);
      if (exception == null) {
        for (final var l : listeners) {
          l.onSendCompleted(metadata);
        }
      }
    }
  }
}
