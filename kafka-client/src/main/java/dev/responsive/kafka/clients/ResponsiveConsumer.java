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
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveConsumer<K, V> extends DelegatingConsumer<K, V> {
  private final List<Listener> listeners;
  private final Logger logger;

  private static class RebalanceListener implements ConsumerRebalanceListener {
    private final Optional<ConsumerRebalanceListener> wrappedRebalanceListener;
    private final List<Listener> listeners;
    private final Logger logger;

    public RebalanceListener(final List<Listener> listeners, final Logger logger) {
      this(Optional.empty(), listeners, logger);
    }

    public RebalanceListener(
        final ConsumerRebalanceListener wrappedRebalanceListener,
        final List<Listener> listeners,
        final Logger logger) {
      this(Optional.of(wrappedRebalanceListener), listeners, logger);
    }

    private RebalanceListener(
        final Optional<ConsumerRebalanceListener> wrappedRebalanceListener,
        final List<Listener> listeners,
        final Logger logger) {
      this.wrappedRebalanceListener = wrappedRebalanceListener;
      this.listeners = listeners;
      this.logger = logger;
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsLost(partitions));
      for (final var l : listeners) {
        ignoreException(logger, () -> l.onPartitionsLost(partitions));
      }
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsRevoked(partitions));
      for (final var l : listeners) {
        ignoreException(logger, () -> l.onPartitionsRevoked(partitions));
      }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
      wrappedRebalanceListener.ifPresent(l -> l.onPartitionsAssigned(partitions));
      for (final var l : listeners) {
        ignoreException(logger, () -> l.onPartitionsAssigned(partitions));
      }
    }
  }

  public ResponsiveConsumer(
      final String clientId, final Consumer<K, V> delegate, final List<Listener> listeners) {
    super(delegate);
    this.logger =
        LoggerFactory.getLogger(
            ResponsiveConsumer.class.getName() + "." + Objects.requireNonNull(clientId));
    this.listeners = Objects.requireNonNull(listeners);
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    delegate.subscribe(topics, new RebalanceListener(listeners, logger));
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
    delegate.subscribe(topics, new RebalanceListener(callback, listeners, logger));
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
    delegate.subscribe(pattern, new RebalanceListener(callback, listeners, logger));
  }

  @Override
  public void subscribe(final Pattern pattern) {
    delegate.subscribe(pattern, new RebalanceListener(listeners, logger));
  }

  @Override
  public void unsubscribe() {
    delegate.unsubscribe();
  }

  @Override
  public void close() {
    delegate.close();
    listeners.forEach(l -> ignoreException(l::onClose));
  }

  @Override
  public void close(final Duration timeout) {
    delegate.close(timeout);
    listeners.forEach(l -> ignoreException(l::onClose));
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
    delegate.commitSync(offsets);
    listeners.forEach(l -> l.onCommit(offsets));
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    delegate.commitSync(offsets, timeout);
    listeners.forEach(l -> l.onCommit(offsets));
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
  public void commitAsync(
      Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("ResponsiveConsumer only supports commitSync");
  }

  private void ignoreException(final Runnable r) {
    ignoreException(logger, r);
  }

  private static void ignoreException(final Logger logger, final Runnable r) {
    try {
      r.run();
    } catch (final Throwable t) {
      logger.error("error calling rebalance listener", t);
    }
  }

  interface Listener {
    default void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

    default void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    default void onPartitionsLost(Collection<TopicPartition> partitions) {}

    default void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {}

    default void onClose() {}
  }
}
