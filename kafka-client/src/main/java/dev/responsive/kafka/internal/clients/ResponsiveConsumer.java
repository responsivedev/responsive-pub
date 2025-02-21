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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class ResponsiveConsumer<K, V> extends DelegatingConsumer<K, V> {
  private final Logger log;

  private final List<Listener> listeners;

  private static class RebalanceListener implements ConsumerRebalanceListener {
    private final ConsumerRebalanceListener wrappedRebalanceListener;
    private final List<Listener> listeners;
    private final Logger log;

    public RebalanceListener(
        final ConsumerRebalanceListener wrappedRebalanceListener,
        final List<Listener> listeners,
        final Logger log
    ) {
      this.wrappedRebalanceListener = wrappedRebalanceListener;
      this.listeners = listeners;
      this.log = log;
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> partitions) {
      for (final var l : listeners) {
        ignoreException(log, () -> l.onPartitionsLost(partitions));
      }
      wrappedRebalanceListener.onPartitionsLost(partitions);
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
      for (final var l : listeners) {
        ignoreException(log, () -> l.onPartitionsRevoked(partitions));
      }
      wrappedRebalanceListener.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
      for (final var l : listeners) {
        ignoreException(log, () -> l.onPartitionsAssigned(partitions));
      }
      wrappedRebalanceListener.onPartitionsAssigned(partitions);
    }
  }

  public ResponsiveConsumer(
      final String clientId,
      final Consumer<K, V> delegate,
      final List<Listener> listeners
  ) {
    super(delegate);
    this.log = new LogContext(
        String.format("responsive-consumer [%s]", Objects.requireNonNull(clientId))
    ).logger(ResponsiveConsumer.class);
    this.listeners = Objects.requireNonNull(listeners);
  }

  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(final long timeout) {
    return poll(Duration.ofMillis(timeout));
  }

  @Override
  public ConsumerRecords<K, V> poll(final Duration timeout) {
    var records = super.poll(timeout);
    for (final Listener listener : listeners) {
      records = listener.onPoll(records);
    }
    return records;
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    throw new IllegalStateException("Unexpected call to subscribe(Collection) on main consumer"
                                        + " without a rebalance listener");
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
    super.subscribe(topics, new RebalanceListener(callback, listeners, log));
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
    super.subscribe(pattern, new RebalanceListener(callback, listeners, log));
  }

  @Override
  public void subscribe(final Pattern pattern) {
    throw new IllegalStateException("Unexpected call to subscribe(Pattern) on main consumer"
                                        + " without a rebalance listener");
  }

  @Override
  public void unsubscribe() {
    listeners.forEach(Listener::onUnsubscribe);
    super.unsubscribe();
  }

  @Override
  public void close() {
    super.close();
    listeners.forEach(l -> ignoreException(l::onConsumerClose));
  }

  @Override
  public void close(final Duration timeout) {
    super.close(timeout);
    listeners.forEach(l -> ignoreException(l::onConsumerClose));
  }

  @Override
  public void commitSync() {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commit with offsets");
  }

  @Override
  public void commitSync(Duration timeout) {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commit with offsets");
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    super.commitSync(offsets);
    listeners.forEach(l -> l.onConsumerCommit(offsets));
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets,
                         Duration timeout) {
    super.commitSync(offsets, timeout);
    listeners.forEach(l -> l.onConsumerCommit(offsets));
  }

  @Override
  public void commitAsync() {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commit with offsets");
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commit with offsets");
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                          OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commitSync");
  }

  private void ignoreException(final Runnable r) {
    ignoreException(log, r);
  }

  private static void ignoreException(final Logger logger, final Runnable r) {
    try {
      r.run();
    } catch (final Throwable t) {
      logger.error("error calling rebalance listener", t);
    }
  }

  public interface Listener {

    default <K, V> ConsumerRecords<K, V> onPoll(ConsumerRecords<K, V> records) {
      return records;
    }

    default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsLost(Collection<TopicPartition> partitions) {
    }

    default void onConsumerCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    default void onUnsubscribe() {
    }

    default void onConsumerClose() {
    }
  }
}
