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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * on ResponsiveConsumer. This class should not be assumed to be thread-safe by callers.
 * Internally, it runs asynchronous poller tasks and synchronizes access to its own shared
 * state.
 */
public class EndOffsetsPoller {
  private static final Logger LOGGER = LoggerFactory.getLogger(EndOffsetsPoller.class);

  private final Map<String, Listener> threadIdToMetrics = new HashMap<>();
  private final Metrics metrics;
  private final Map<String, Object> configs;
  private final Factories factories;
  private final ScheduledExecutorService executor;
  private Poller poller = null;

  public EndOffsetsPoller(
      final Map<String, ?> configs,
      final Metrics metrics
  ) {
    this(
        configs,
        metrics,
        Executors.newSingleThreadScheduledExecutor(r -> {
          final var t = new Thread(r);
          t.setDaemon(true);
          return t;
        }),
        new Factories() {}
    );
  }

  EndOffsetsPoller(
      final Map<String, ?> configs,
      final Metrics metrics,
      final ScheduledExecutorService executor,
      final Factories factories
  ) {
    // TODO: make sure to use a read-committed consumer
    this.configs = consumerConfigs(Objects.requireNonNull(configs));
    this.metrics = Objects.requireNonNull(metrics);
    this.executor = Objects.requireNonNull(executor);
    this.factories = Objects.requireNonNull(factories);
  }

  private Map<String, Object> consumerConfigs(final Map<String, ?> providedConfigs) {
    final HashMap<String, Object> configs = new HashMap<>(providedConfigs);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
    return Map.copyOf(configs);
  }

  public synchronized Listener addForThread(final String threadId) {
    LOGGER.info("add end offset metrics for thread {}", threadId);
    final var tm = new Listener(threadId, metrics, this::removeForThread);
    if (threadIdToMetrics.containsKey(threadId)) {
      final var err = String.format("end offset poller already has metrics for %s", threadId);
      LOGGER.error(err);
      throw new RuntimeException(err);
    }
    if (threadIdToMetrics.isEmpty()) {
      initPoller();
    }
    threadIdToMetrics.put(threadId, tm);
    return tm;
  }

  private synchronized void removeForThread(final String threadId) {
    final Listener tm = threadIdToMetrics.remove(threadId);
    if (tm != null) {
      if (threadIdToMetrics.isEmpty()) {
        stopPoller();
      }
    } else {
      LOGGER.warn("no metrics found for thread {}", threadId);
    }
  }

  private synchronized Collection<Listener> getThreadMetrics() {
    return List.copyOf(threadIdToMetrics.values());
  }

  private void initPoller() {
    LOGGER.info("init end offsets poller");
    if (poller != null) {
      throw new IllegalStateException("poller already initialized");
    }
    poller = new Poller(factories.createAdminClient(configs), executor, this::getThreadMetrics);
  }

  private void stopPoller() {
    LOGGER.info("stopping end offsets poller");
    try {
      poller.stop();
    } catch (final RuntimeException e) {
      LOGGER.warn("poller stop returned an unexpected error. It will be ignored, and the poller "
          + "task + admin client might be leaked.", e);
    }
    poller = null;
  }

  private static class Poller {
    private final Admin adminClient;
    private final ScheduledFuture<?> future;
    private final ScheduledExecutorService executor;
    private final Supplier<Collection<Listener>> threadMetricsSupplier;

    private Poller(
        final Admin adminClient,
        final ScheduledExecutorService executor,
        final Supplier<Collection<Listener>> threadMetricsSupplier
    ) {
      this.adminClient = adminClient;
      this.executor = executor;
      this.threadMetricsSupplier = threadMetricsSupplier;
      this.future = executor.scheduleAtFixedRate(this::pollEndOffsets, 0, 30, TimeUnit.SECONDS);
    }

    private void stop() {
      future.cancel(true);
      executor.schedule(() -> adminClient.close(Duration.ofNanos(0)), 0, TimeUnit.SECONDS);
    }

    private void pollEndOffsets() {
      final var partitions = new HashMap<TopicPartition, OffsetSpec>();
      final Collection<Listener> threadMetrics = threadMetricsSupplier.get();
      for (final var tm : threadMetrics) {
        for (final var tp : tm.endOffsets.keySet()) {
          partitions.put(tp, OffsetSpec.latest());
        }
      }
      final var result = adminClient.listOffsets(partitions);
      final Map<TopicPartition, ListOffsetsResultInfo> endOffsets;
      try {
        endOffsets = result.all().get();
      } catch (final InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      threadMetrics.forEach(tm -> tm.update(endOffsets));
    }
  }

  static class Listener implements ResponsiveConsumer.Listener {
    private final String threadId;
    private final Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
    private final Logger logger;
    private final Metrics metrics;
    private final Consumer<String> onClose;


    private Listener(final String threadId, final Metrics metrics, final Consumer<String> onClose) {
      this.threadId = threadId;
      this.metrics = metrics;
      this.onClose = onClose;
      logger = LoggerFactory.getLogger(EndOffsetsPoller.class.getName() + "." + threadId);
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

    private void update(final Map<TopicPartition, ListOffsetsResultInfo> endOffsets) {
      for (final var entry : endOffsets.entrySet()) {
        this.endOffsets.computeIfPresent(entry.getKey(), (k, v) -> {
          logger.info("update end offset of {}/{} to {}",
              k.topic(),
              k.partition(),
              entry.getValue().offset()
          );
          return entry.getValue().offset();
        });
      }
    }

    public void close() {
      logger.info("cleanup offsets for thread {}", threadId);
      for (final TopicPartition p : endOffsets.keySet()) {
        metrics.removeMetric(metricName(p));
      }
      onClose.accept(threadId);
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
    default Admin createAdminClient(final Map<String, Object> configs) {
      return Admin.create(configs);
    }
  }
}
