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

import static dev.responsive.kafka.internal.metrics.TopicMetrics.END_OFFSET;
import static dev.responsive.kafka.internal.metrics.TopicMetrics.END_OFFSET_DESCRIPTION;

import dev.responsive.kafka.internal.clients.ResponsiveConsumer;
import dev.responsive.kafka.internal.utils.Constants;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
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
  private static final Logger LOG = LoggerFactory.getLogger(EndOffsetsPoller.class);
  private static final int ADMIN_RECREATE_THRESHOLD = 3;

  private final Map<String, Listener> threadIdToMetrics = new HashMap<>();
  private final ResponsiveMetrics metrics;
  private final Map<String, Object> configs;
  private final Factories factories;
  private final ScheduledExecutorService executor;
  private Poller poller = null;

  public EndOffsetsPoller(
      final Map<String, ?> configs,
      final ResponsiveMetrics metrics,
      final Factories factories
  ) {
    this(
        configs,
        metrics,
        Executors.newSingleThreadScheduledExecutor(r -> {
          final var t = new Thread(r);
          t.setDaemon(true);
          t.setName("responsive-end-offsets-poller");
          return t;
        }),
        factories
    );
  }

  public EndOffsetsPoller(
      final Map<String, ?> configs,
      final ResponsiveMetrics metrics,
      final ScheduledExecutorService executor,
      final Factories factories
  ) {
    // TODO: does admin client get only committed offsets?
    this.configs = Map.copyOf(Objects.requireNonNull(configs));
    this.metrics = Objects.requireNonNull(metrics);
    this.executor = Objects.requireNonNull(executor);
    this.factories = Objects.requireNonNull(factories);
  }

  public synchronized Listener addForThread(final String threadId) {
    LOG.debug("Adding end offset metrics for thread {}", threadId);
    final var tm = new Listener(threadId, metrics, this::removeForThread);
    if (threadIdToMetrics.containsKey(threadId)) {
      final var msg = String.format("End offset poller already has metrics for %s", threadId);
      final var err = new RuntimeException(msg);
      LOG.error(msg, err);
      throw err;
    }
    if (threadIdToMetrics.isEmpty()) {
      initPoller();
    }
    threadIdToMetrics.put(threadId, tm);
    return tm;
  }

  private synchronized void removeForThread(final String threadId) {
    LOG.debug("Removing end offset metrics for thread {}", threadId);
    final Listener tm = threadIdToMetrics.remove(threadId);
    if (tm != null) {
      if (threadIdToMetrics.isEmpty()) {
        stopPoller();
      }
    } else {
      LOG.warn("No metrics found for thread {}", threadId);
    }
  }

  private synchronized Collection<Listener> getThreadMetrics() {
    return List.copyOf(threadIdToMetrics.values());
  }

  private void initPoller() {
    LOG.info("Initializing end offsets poller");
    if (poller != null) {
      throw new IllegalStateException("Poller was already initialized");
    }
    poller = new Poller(
        () -> factories.createAdminClient(configs),
        executor,
        this::getThreadMetrics
    );
  }

  private void stopPoller() {
    LOG.info("Stopping end offsets poller");
    try {
      poller.stop();
    } catch (final RuntimeException e) {
      LOG.warn("Poller stop returned an unexpected error. It will be ignored, and the poller "
          + "task + admin client might be leaked.", e);
    }
    poller = null;
  }

  private static class Poller {
    private Admin adminClient;
    private int failuresWithoutReinit = 0;
    private final Supplier<Admin> adminSupplier;
    private final ScheduledFuture<?> future;
    private final ScheduledExecutorService executor;
    private final Supplier<Collection<Listener>> threadMetricsSupplier;

    private Poller(
        final Supplier<Admin> adminClientSupplier,
        final ScheduledExecutorService executor,
        final Supplier<Collection<Listener>> threadMetricsSupplier
    ) {
      this.adminSupplier = adminClientSupplier;
      this.executor = executor;
      this.threadMetricsSupplier = threadMetricsSupplier;
      init();
      this.future = executor.scheduleAtFixedRate(this::pollEndOffsets, 0, 30, TimeUnit.SECONDS);
    }

    private void stop() {
      future.cancel(true);
      executor.schedule(this::close, 0, TimeUnit.SECONDS);
    }

    private void init() {
      adminClient = adminSupplier.get();
      failuresWithoutReinit = 0;
    }

    private void close() {
      adminClient.close(Duration.ofNanos(0));
    }

    private void maybeReinit() {
      if (failuresWithoutReinit >= ADMIN_RECREATE_THRESHOLD) {
        LOG.info("reinitializing admin client");
        close();
        init();
      }
    }

    private void doPollEndOffsets() {
      LOG.info("Polling end offsets");

      maybeReinit();

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
        endOffsets = result.all()
            .get(Constants.BLOCKING_TIMEOUT_VALUE, Constants.BLOCKING_TIMEOUT_UNIT);
      } catch (final InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
      threadMetrics.forEach(tm -> tm.update(endOffsets));

      failuresWithoutReinit = 0;

      LOG.info("Finished updating end offsets");
    }

    private void pollEndOffsets() {
      try {
        doPollEndOffsets();
      } catch (final RuntimeException e) {
        LOG.error("error polling end offsets. will retry at next poll interval", e);
        failuresWithoutReinit += 1;
      }
    }
  }

  public static class Listener implements ResponsiveConsumer.Listener {
    private final String threadId;
    private final Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
    private final Logger log;
    private final ResponsiveMetrics metrics;
    private final Consumer<String> onClose;

    private Listener(
        final String threadId,
        final ResponsiveMetrics metrics,
        final Consumer<String> onClose
    ) {
      this.threadId = threadId;
      this.metrics = metrics;
      this.onClose = onClose;
      log = LoggerFactory.getLogger(EndOffsetsPoller.class.getName() + "." + threadId);
    }

    @Override
    public void onPartitionsLost(final Collection<TopicPartition> lost) {
      // For now, we can just delegate to #onPartitionsRevoked here since we're just
      // cleaning up partitions we no longer own, but these callbacks are semantically
      // distinct and may need individual implementations if more sophisticated metrics
      // are ever introduced
      onPartitionsRevoked(lost);
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> revoked) {
      for (final var p : revoked) {
        metrics.removeMetric(endOffsetMetric(p));
        endOffsets.remove(p);
      }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> assigned) {
      for (final var p : assigned) {
        endOffsets.put(p, -1L);
        metrics.addMetric(
            endOffsetMetric(p),
            (Gauge<Long>) (config, now) -> endOffsets.getOrDefault(p, -1L)
        );
      }
    }

    private void update(final Map<TopicPartition, ListOffsetsResultInfo> newEndOffsets) {
      for (final var entry : newEndOffsets.entrySet()) {
        endOffsets.computeIfPresent(entry.getKey(), (k, v) -> entry.getValue().offset());
      }

      if (log.isDebugEnabled()) {
        log.debug("Updated end offsets to [{}].",
                  endOffsets.entrySet().stream()
                     .map(tp -> "(" + tp.getKey() + ": " + tp.getValue() + ")")
                     .collect(Collectors.joining(", "))
        );
      }
    }

    public void close() {
      log.info("Cleaning up offset metrics");
      for (final TopicPartition p : endOffsets.keySet()) {
        metrics.removeMetric(endOffsetMetric(p));
      }
      onClose.accept(threadId);
    }

    private MetricName endOffsetMetric(final TopicPartition topicPartition) {
      return metrics.metricName(
          END_OFFSET,
          END_OFFSET_DESCRIPTION,
          metrics.topicLevelMetric(TopicMetrics.TOPIC_METRIC_GROUP, threadId, topicPartition)
      );
    }
  }

  @FunctionalInterface
  public interface Factories {
    Admin createAdminClient(final Map<String, Object> configs);
  }
}
