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

package dev.responsive.kafka.internal.clients;


import dev.responsive.kafka.internal.utils.GroupTraceRoot;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
  private final GroupTraceRoot traceRoot;

  private static class RebalanceListener implements ConsumerRebalanceListener {
    private final ConsumerRebalanceListener wrappedRebalanceListener;
    private final List<Listener> listeners;
    private final Logger log;
    private final GroupTraceRoot traceRoot;
    private final ResponsiveConsumer<?, ?> consumer;

    public RebalanceListener(
        final ConsumerRebalanceListener wrappedRebalanceListener,
        final List<Listener> listeners,
        final Logger log,
        final GroupTraceRoot traceRoot,
        final ResponsiveConsumer<?, ?> consumer
    ) {
      this.wrappedRebalanceListener = wrappedRebalanceListener;
      this.listeners = listeners;
      this.log = log;
      this.traceRoot = traceRoot;
      this.consumer = consumer;
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
      traceRoot.refresh(consumer.groupMetadata().generationId());
      final var spans = partitions.stream()
          .map(p -> {
            final var partitionSpan = traceRoot.rootSpan().childSpan("on-partitions-revoked");
            partitionSpan.span().setAttribute("partition", p.toString());
            return partitionSpan;
          })
          .collect(Collectors.toList());
      for (final var l : listeners) {
        ignoreException(log, () -> l.onPartitionsRevoked(partitions));
      }
      try {
        wrappedRebalanceListener.onPartitionsRevoked(partitions);
      } catch (final RuntimeException e) {
        for (final var span: spans) {
          span.span().recordException(e);
        }
        throw e;
      } finally {
        for (final var span: spans) {
          span.end();
        }
      }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
      traceRoot.refresh(consumer.groupMetadata().generationId());
      final var spans = partitions.stream()
          .map(p -> {
            final var partitionSpan = traceRoot.rootSpan().childSpan("on-partitions-assigned");
            partitionSpan.span().setAttribute("partition", p.toString());
            return partitionSpan;
          })
          .collect(Collectors.toList());
      for (final var l : listeners) {
        ignoreException(log, () -> l.onPartitionsAssigned(partitions));
      }
      try {
        wrappedRebalanceListener.onPartitionsAssigned(partitions);
      } catch (final RuntimeException e) {
        for (final var span: spans) {
          span.span().recordException(e);
        }
        throw e;
      } finally {
        for (final var span: spans) {
          span.end();
        }
      }
    }
  }

  public ResponsiveConsumer(
      final String clientId,
      final Consumer<K, V> delegate,
      final List<Listener> listeners,
      final GroupTraceRoot traceRoot
  ) {
    super(delegate);
    this.log = new LogContext(
        String.format("responsive-consumer [%s]", Objects.requireNonNull(clientId))
    ).logger(ResponsiveConsumer.class);
    this.listeners = Objects.requireNonNull(listeners);
    this.traceRoot = traceRoot;
  }

  @Override
  public void subscribe(final Collection<String> topics) {
    throw new IllegalStateException("Unexpected call to subscribe(Collection) on main consumer"
                                        + " without a rebalance listener");
  }

  @Override
  public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
    super.subscribe(
        topics,
        new RebalanceListener(callback, listeners, log, traceRoot, this));
  }

  @Override
  public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
    super.subscribe(
        pattern,
        new RebalanceListener(callback, listeners, log, traceRoot, this)
    );
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
    listeners.forEach(l -> ignoreException(l::onClose));
  }

  @Override
  public void close(final Duration timeout) {
    super.close(timeout);
    listeners.forEach(l -> ignoreException(l::onClose));
  }

  @SuppressWarnings("deprecation")
  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    GroupTraceRoot.INSTANCE.set(traceRoot);
    traceRoot.refresh(groupMetadata().generationId());
    final var result = super.poll(timeout);
    traceRoot.refresh(groupMetadata().generationId());
    return result;
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    GroupTraceRoot.INSTANCE.set(traceRoot);
    traceRoot.refresh(groupMetadata().generationId());
    final var result = super.poll(timeout);
    traceRoot.refresh(groupMetadata().generationId());
    return result;
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
    listeners.forEach(l -> l.onCommit(offsets));
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets,
                         Duration timeout) {
    super.commitSync(offsets, timeout);
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
    default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    default void onPartitionsLost(Collection<TopicPartition> partitions) {
    }

    default void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    default void onUnsubscribe() {
    }

    default void onClose() {
    }
  }
}
