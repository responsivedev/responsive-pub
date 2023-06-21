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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class polls the current end offset of the topic partitions assigned locally and
 * publishes the values as a metric. It gets the set of assigned partitions by listening
 * on ResponsiveConsumer.
 *
 */
public class EndOffsetsPoller {
  private static final Logger LOGGER = LoggerFactory.getLogger(EndOffsetsPoller.class);

  private final Map<String, ThreadMetrics> threadIdToMetrics = new HashMap<>();
  private final Metrics metrics;
  private final Map<String, Object> configs;
  private final Factories factories;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> pollFuture;
  private Consumer<?, ?> consumer;

  public EndOffsetsPoller(
      final Map<String, ?> configs,
      final Metrics metrics
  ) {
    this(configs, metrics, new Factories() {});
  }

  EndOffsetsPoller(
      final Map<String, ?> configs,
      final Metrics metrics,
      final Factories factories
  ) {
    // TODO: make sure to use a read-committed consumer
    this.configs = consumerConfigs(Objects.requireNonNull(configs));
    this.metrics = Objects.requireNonNull(metrics);
    this.factories = Objects.requireNonNull(factories);
  }

  private Map<String, Object> consumerConfigs(final Map<String, ?> providedConfigs) {
    final HashMap<String, Object> configs = new HashMap<>(providedConfigs);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    return Map.copyOf(configs);
  }

  public ResponsiveConsumer.Listener addForThread(final String threadId) {
    LOGGER.info("add end offset metrics for thread {}", threadId);
    final var tm = new ThreadMetrics(threadId);
    synchronized (this) {
      assert !threadIdToMetrics.containsKey(threadId);
      if (threadIdToMetrics.isEmpty()) {
        initPoller();
        threadIdToMetrics.put(threadId, tm);
      }
    }
    return tm;
  }

  public void removeForThread(final String threadId) {
    final ThreadMetrics tm;
    synchronized (this) {
      tm = threadIdToMetrics.remove(threadId);
      if (tm != null) {
        tm.cleanup();
        if (threadIdToMetrics.isEmpty()) {
          stopPoller();
        }
      }
    }
  }

  private void pollEndOffsets() {
    final Set<TopicPartition> partitions = new HashSet<>();
    final Collection<ThreadMetrics> threadMetrics;
    synchronized (this) {
      threadMetrics = List.copyOf(this.threadIdToMetrics.values());
    }
    for (final var tm : threadMetrics) {
      partitions.addAll(tm.endOffsets.keySet());
    }
    final var endOffsets = consumer.endOffsets(partitions);
    threadMetrics.forEach(tm -> tm.update(endOffsets));
  }

  private void initPoller() {
    LOGGER.info("init end offsets poller");
    assert consumer == null;
    assert executor == null;
    consumer = factories.createConsumer(configs);
    executor = factories.createExecutor();
    pollFuture = executor.scheduleAtFixedRate(this::pollEndOffsets, 0, 30, TimeUnit.SECONDS);
  }

  private void stopPoller() {
    LOGGER.info("stopping end offsets poller");
    try {
      consumer.close(Duration.ofNanos(0));
    } catch (final Exception e) {
      LOGGER.warn("error closing consumer", e);
    }
    consumer = null;
    pollFuture.cancel(true);
    executor.shutdown();
    executor = null;
  }

  private class ThreadMetrics implements ResponsiveConsumer.Listener {
    private final String threadId;
    private final Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();

    private ThreadMetrics(final String threadId) {
      this.threadId = Objects.requireNonNull(threadId);
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> lost) {
      onPartitionsRevoked(lost);
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> revoked) {
      for (final var p : revoked) {
        metrics.removeMetric(metricName(p));
        endOffsets.remove(p);
      }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> assigned) {
      for (final var p : assigned) {
        endOffsets.put(p, -1L);
        metrics.addMetric(
            metricName(p),
            (Gauge<Long>) (config, now) -> endOffsets.getOrDefault(p, -1L)
        );
      }
    }

    private void update(final Map<TopicPartition, Long> endOffsets) {
      for (final var tp : endOffsets.keySet()) {
        this.endOffsets.computeIfPresent(tp, (k, v) -> {
          LOGGER.info("update end offset of {}/{} to {}",
              tp.topic(),
              tp.partition(),
              endOffsets.get(tp)
          );
          return endOffsets.get(tp);
        });
      }
    }

    private void cleanup() {
      LOGGER.info("cleanup offsets for thread {}", threadId);
      for (final TopicPartition p : endOffsets.keySet()) {
        metrics.removeMetric(metricName(p));
      }
    }

    private MetricName metricName(final TopicPartition topicPartition) {
      final var tags = new HashMap<String, String>();
      tags.put("thread", threadId);
      tags.put("topic", topicPartition.topic());
      tags.put("partition", Integer.toString(topicPartition.partition()));
      return new MetricName("end-offset", "responsive.streams", "", tags);
    }
  }

  interface Factories {
    default KafkaConsumer<byte[], byte[]> createConsumer(final Map<String, Object> configs) {
      return new KafkaConsumer<>(configs);
    }

    default ScheduledExecutorService createExecutor() {
      return Executors.newSingleThreadScheduledExecutor(r -> {
        final var t = new Thread(r);
        t.setDaemon(true);
        return t;
      });
    }
  }
}
