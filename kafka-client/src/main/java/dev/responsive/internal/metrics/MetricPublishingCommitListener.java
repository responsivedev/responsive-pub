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

package dev.responsive.internal.metrics;

import dev.responsive.internal.clients.OffsetRecorder;
import dev.responsive.internal.clients.OffsetRecorder.RecordingKey;
import dev.responsive.internal.clients.ResponsiveConsumer.Listener;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
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
  private final Metrics metrics;
  private final String threadId;
  private final String consumerGroup;
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
      final Metrics metrics,
      final String threadId,
      final String consumerGroup,
      final OffsetRecorder offsetRecorder
  ) {
    this.metrics = Objects.requireNonNull(metrics);
    this.threadId = Objects.requireNonNull(threadId);
    this.consumerGroup = Objects.requireNonNull(consumerGroup);
    offsetRecorder.addCommitCallback(this::commitCallback);
  }

  private MetricName metricName(final TopicPartition partition) {
    final Map<String, String> tags = new HashMap<>();
    tags.put("consumerGroup", consumerGroup);
    tags.put("thread", threadId);
    tags.put("topic", partition.topic());
    tags.put("partition", Integer.toString(partition.partition()));
    return new MetricName("committed-offset", "responsive.streams", "", tags);
  }

  private void commitCallback(
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
        metrics.removeMetric(metricName(k));
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
            metrics.addMetric(metricName(k), (Gauge<Long>) (config, now) -> getCommittedOffset(k));
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
        metrics.removeMetric(metricName(p));
      }
    }
    offsets.clear();
  }
}
