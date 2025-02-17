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

package dev.responsive.kafka.internal.metrics;

import static dev.responsive.kafka.internal.metrics.TopicMetrics.COMMITTED_OFFSET;
import static dev.responsive.kafka.internal.metrics.TopicMetrics.COMMITTED_OFFSET_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.TopicMetrics.TOPIC_METRIC_GROUP;

import dev.responsive.kafka.internal.clients.OffsetRecorder;
import dev.responsive.kafka.internal.clients.OffsetRecorder.RecordingKey;
import dev.responsive.kafka.internal.clients.ResponsiveConsumer.Listener;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class publishes a metric that provides the current committed offset for a given
 * topic-partition. It implements ResponsiveProducer.Listener and ResponsiveConsumer.Listener
 * to listen on commit events and partition assignment changes, respectively. On a partition
 * assignment change, it adds or removes metric instances. Each metric instance returns the
 * current committed offset provided by the producer callback.
 *
 * Synchronization: All shared access goes through the "offsets" map.
 */
public class MetricPublishingCommitListener implements Listener, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricPublishingCommitListener.class);
  private final ResponsiveMetrics metrics;
  private final String threadId;
  private final Map<TopicPartition, CommittedOffset> offsets = new ConcurrentHashMap<>();

  private static class CommittedOffset {
    private final TopicPartition topicPartition;
    private final Long offset;

    private CommittedOffset(
        final TopicPartition topicPartition,
        final Long offset
    ) {
      this.topicPartition = topicPartition;
      this.offset = offset;
    }

    public OptionalLong getOffset() {
      return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
    }
  }

  public MetricPublishingCommitListener(
      final ResponsiveMetrics metrics,
      final String threadId,
      final OffsetRecorder offsetRecorder
  ) {
    this.metrics = Objects.requireNonNull(metrics);
    this.threadId = Objects.requireNonNull(threadId);
    offsetRecorder.addCommitCallback(this::commitCallback);
  }

  private MetricName committedOffsetMetric(final TopicPartition topicPartition) {
    return metrics.metricName(
        COMMITTED_OFFSET,
        COMMITTED_OFFSET_DESCRIPTION,
        metrics.topicLevelMetric(TOPIC_METRIC_GROUP, threadId, topicPartition));
  }

  private void commitCallback(
      final String threadId,
      final Map<RecordingKey, Long> committedOffsets,
      final Map<TopicPartition, Long> unused
  ) {
    for (final var e : committedOffsets.entrySet()) {
      offsets.computeIfPresent(
          e.getKey().getPartition(),
          (k, v) -> {
            LOG.debug("record committed offset {} {}: {}", threadId, k, e.getValue());
            return new CommittedOffset(k, e.getValue());
          }
      );
    }
  }

  @Override
  public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
    LOG.info("Remove committed offset metrics entry for {}",
        partitions.stream()
            .filter(offsets::containsKey)
            .map(TopicPartition::toString)
            .collect(Collectors.joining(","))
    );
    for (final var p : partitions) {
      offsets.computeIfPresent(p, (k, v) -> {
        metrics.removeMetric(committedOffsetMetric(k));
        return null;
      });
    }
  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
    LOG.info("Add committed offsets metrics entry for {}",
        partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(","))
    );
    for (final var p : partitions) {
      offsets.computeIfAbsent(
          p,
          k -> {
            LOG.debug("add committed offset metric for {} {}", threadId, k);
            metrics.addMetric(
                committedOffsetMetric(k), (Gauge<Long>) (config, now) -> getCommittedOffset(k));
            return new CommittedOffset(
                p,
                null
            );
          }
      );
    }
  }

  private Long getCommittedOffset(final TopicPartition partition) {
    final CommittedOffset committedOffset = offsets.get(partition);
    if (committedOffset == null || committedOffset.getOffset().isEmpty()) {
      return -1L;
    }
    return committedOffset.getOffset().getAsLong();
  }

  @Override
  public void onPartitionsLost(final Collection<TopicPartition> partitions) {
    onPartitionsRevoked(partitions);
  }

  @Override
  public void close() {
    // at this point we assume no threads will call the other callbacks
    for (final TopicPartition p : offsets.keySet()) {
      if (offsets.containsKey(p)) {
        LOG.info("Clean up committed offset metric {} {}", threadId, p);
        metrics.removeMetric(committedOffsetMetric(p));
      }
    }
    offsets.clear();
  }
}
