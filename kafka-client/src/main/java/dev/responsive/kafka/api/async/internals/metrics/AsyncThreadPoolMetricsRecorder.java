package dev.responsive.kafka.api.async.internals.metrics;

import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;

public class AsyncThreadPoolMetricsRecorder {
  public static final String GROUP_NAME = "async-processor-thread-pool-metrics";

  public static final String QUEUED_EVENTS = "queued-events";
  public static final String QUEUED_EVENTS_DESC
      = "The total number of events pending in the async thread pool queue";

  private final ResponsiveMetrics metrics;
  private final MetricName poolQueueSizeGauge;

  public AsyncThreadPoolMetricsRecorder(
      final ResponsiveMetrics responsiveMetrics,
      final String threadId,
      final Supplier<Integer> poolQueueSize
  ) {
    this.metrics = Objects.requireNonNull(responsiveMetrics);
    final var scope = responsiveMetrics.threadLevelMetric(GROUP_NAME, threadId);
    poolQueueSizeGauge = metrics.metricName(QUEUED_EVENTS, QUEUED_EVENTS_DESC, scope);
    metrics.addMetric(poolQueueSizeGauge, (Gauge<Integer>) (t, c) -> poolQueueSize.get());
  }

  public void close() {
    metrics.removeMetric(poolQueueSizeGauge);
  }
}
