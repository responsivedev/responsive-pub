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

import dev.responsive.kafka.clients.OffsetRecorder.RecordingKey;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
public class MetricPublishingCommitListener implements ResponsiveConsumer.Listener, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricPublishingCommitListener.class);
  private final Metrics metrics;
  private final String threadId;
  private final Map<TopicPartition, Optional<CommittedOffset>> offsets = new ConcurrentHashMap<>();

  private static class CommittedOffset {
    private final long offset;
    private final String consumerGroup;

    private CommittedOffset(final long offset, final String consumerGroup) {
      this.offset = offset;
      this.consumerGroup = consumerGroup;
    }

    public long getOffset() {
      return offset;
    }

    public String getConsumerGroup() {
      return consumerGroup;
    }
  }

  public MetricPublishingCommitListener(
      final Metrics metrics,
      final String threadId,
      final OffsetRecorder offsetRecorder
  ) {
    this.metrics = Objects.requireNonNull(metrics);
    this.threadId = Objects.requireNonNull(threadId);
    offsetRecorder.addCommitCallback(this::commitCallback);
  }

  private MetricName metricName(final RecordingKey k) {
    return metricName(k.getPartition(), k.getConsumerGroup());
  }

  private MetricName metricName(final TopicPartition partition, final String consumerGroup) {
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
            if (v.isEmpty()) {
              LOG.debug("add committed offset metric for {} {}", threadId, k);
              metrics.addMetric(
                  metricName(e.getKey()),
                  (Gauge<Long>) (config, now) ->
                      offsets.getOrDefault(k, Optional.empty())
                          .map(CommittedOffset::getOffset).orElse(-1L)
              );
            }
            LOG.debug("record committed offset {} {}: {}", threadId, k, e.getValue());
            return Optional.of(new CommittedOffset(e.getValue(), e.getKey().getConsumerGroup()));
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
        v.ifPresent(offset -> metrics.removeMetric(metricName(k, offset.getConsumerGroup())));
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
      offsets.putIfAbsent(p, Optional.empty());
    }
  }

  @Override
  public void onPartitionsLost(final Collection<TopicPartition> partitions) {
    onPartitionsRevoked(partitions);
  }

  @Override
  public void close() {
    // at this point we assume no threads will call the other callbacks
    for (final TopicPartition p : offsets.keySet()) {
      if (offsets.get(p).isPresent()) {
        LOG.info("Clean up committed offset metric {} {}", threadId, p);
        metrics.removeMetric(metricName(p, offsets.get(p).get().getConsumerGroup()));
      }
    }
    offsets.clear();
  }
}
