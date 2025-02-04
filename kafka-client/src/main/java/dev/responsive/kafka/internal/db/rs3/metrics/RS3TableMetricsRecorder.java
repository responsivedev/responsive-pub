package dev.responsive.kafka.internal.db.rs3.metrics;

import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;

public class RS3TableMetricsRecorder {
  public static final String GROUP_NAME = "rs3-table-metrics";

  private static final String GET_SENSOR_NAME = "get-sensor";
  private static final String GET_LATENCY_NS_AVG = "get-latency-ns-avg";
  private static final String GET_LATENCY_NS_AVG_DESC = "average rs3 get latency in nanos";

  private final ResponsiveMetrics metrics;

  private final Sensor getSensor;

  public RS3TableMetricsRecorder(
      final ResponsiveMetrics metrics,
      final ResponsiveMetrics.MetricScopeBuilder metricsScopeBuilder
  ) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    ResponsiveMetrics.MetricScope metricScope = metricsScopeBuilder.build(GROUP_NAME);
    this.getSensor = this.metrics.addSensor(metricScope.sensorName(GET_SENSOR_NAME));
    getSensor.add(
        metrics.metricName(GET_LATENCY_NS_AVG, GET_LATENCY_NS_AVG_DESC, metricScope),
        new Avg()
    );
  }

  public void recordGet(final Duration latency) {
    getSensor.record(latency.toNanos());
  }

  public void close() {
    this.metrics.removeSensor(GET_SENSOR_NAME);
  }
}
