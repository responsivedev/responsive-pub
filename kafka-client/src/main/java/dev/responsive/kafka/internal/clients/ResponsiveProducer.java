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

public class ResponsiveProducer<K, V> extends DelegatingProducer<K, V> {
  private final List<Listener> listeners;
  private final Logger logger;

  public interface Listener {
    default void onProducerCommit() {
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

    default void onProducerClose() {
    }

    default <K, V> ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
      return record;
    }
  }

  public ResponsiveProducer(
      final String clientid,
      final Producer<K, V> wrapped,
      final List<Listener> listeners
  ) {
    super(wrapped);
    this.logger = LoggerFactory.getLogger(
        ResponsiveProducer.class.getName() + "." + Objects.requireNonNull(clientid));
    this.listeners = Objects.requireNonNull(listeners);
  }
  
  @Override
  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsets,
      final ConsumerGroupMetadata groupMetadata
  ) throws ProducerFencedException {
    super.sendOffsetsToTransaction(offsets, groupMetadata);
    for (final var l : listeners) {
      l.onSendOffsetsToTransaction(offsets, groupMetadata.groupId());
    }
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    super.commitTransaction();
    listeners.forEach(Listener::onProducerCommit);
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    super.abortTransaction();
    listeners.forEach(Listener::onAbort);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    for (final Listener listener : listeners) {
      record = listener.onSend(record);
    }
    return new RecordingFuture(super.send(record), listeners);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback callback) {
    for (final Listener listener : listeners) {
      record = listener.onSend(record);
    }
    return new RecordingFuture(
        super.send(record, new RecordingCallback(callback, listeners)), listeners
    );
  }

  @Override
  public void close() {
    super.close();
    closeListeners();
  }

  @Override
  public void close(final Duration timeout) {
    super.close();
    closeListeners();
  }

  private void closeListeners() {
    // TODO(rohan): use consistent error behaviour on all callbacks - just throw up
    for (final var l : listeners) {
      try {
        l.onProducerClose();
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
