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

package dev.responsive.kafka.api.async.internals.metrics;

import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.AVG_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.AVG_SUFFIX;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MAX_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MAX_SUFFIX;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.processor.TaskId;

public class AsyncProcessorMetricsRecorder {
  public static final String GROUP_NAME = "async-processor-metrics";

  public static final String PENDING_EVENTS = "pending-events";
  public static final String PENDING_EVENTS_DESC
      = "The current number of events pending in processor";

  public static final String PROCESS_TIME = "event-process-duration-ns";
  public static final String PROCESS_TIME_MAX = PROCESS_TIME + MAX_SUFFIX;
  public static final String PROCESS_TIME_AVG = PROCESS_TIME + AVG_SUFFIX;
  public static final String PROCESS_TIME_DESC = "time to process a record";
  public static final String PROCESS_TIME_MAX_DESC = MAX_DESCRIPTION + PROCESS_TIME_DESC;
  public static final String PROCESS_TIME_AVG_DESC = AVG_DESCRIPTION + PROCESS_TIME_DESC;

  public static final String TRANSITION_TIME = "event-transition-duration-ns";
  public static final String TRANSITION_TIME_MAX = TRANSITION_TIME + MAX_SUFFIX;
  public static final String TRANSITION_TIME_AVG = TRANSITION_TIME + AVG_SUFFIX;
  public static final String TRANSITION_TIME_DESC = "time to transition between states";
  public static final String TRANSITION_TIME_MAX_DESC = MAX_DESCRIPTION + TRANSITION_TIME_DESC;
  public static final String TRANSITION_TIME_AVG_DESC = AVG_DESCRIPTION + TRANSITION_TIME_DESC;

  public static final String SCHEDULING_QUEUE_SIZE = "scheduling-queue-size";
  public static final String SCHEDULING_QUEUE_SIZE_DESC = "entries in the scheduling queue";
  public static final String SCHEDULING_QUEUE_LONGEST_SIZE = "scheduling-queue-longest-size";
  public static final String SCHEDULING_QUEUE_LONGEST_SIZE_DESC
      = "entries in the queue for key w/ most entries in scheduling queue";

  public static final String FROM_STATE = "from-state";
  public static final String TO_STATE = "to-state";

  private final ResponsiveMetrics.MetricScope scope;
  private final ResponsiveMetrics metrics;
  private final MetricName pendingEventsGauge;
  private final Sensor eventProcessSensor;
  private final Sensor schedulingQueueSizeSensor;
  private final Sensor schedulingQueueLongestQueueSizeSensor;
  private final ConcurrentMap<String, Sensor> stateTransitionSensors = new ConcurrentHashMap<>();

  public AsyncProcessorMetricsRecorder(
      final String threadId,
      final TaskId taskId,
      final String processorName,
      final ResponsiveMetrics metrics,
      final Supplier<Integer> pendingEventsSize
  ) {
    this.metrics = metrics;
    scope = metrics.processorLevelMetric(
        GROUP_NAME, threadId, taskId, processorName);

    this.pendingEventsGauge = metrics.metricName(PENDING_EVENTS, PENDING_EVENTS_DESC, scope);
    metrics.addMetric(pendingEventsGauge, (Gauge<Integer>) (c, n) -> pendingEventsSize.get());

    this.eventProcessSensor = metrics.addSensor(scope.sensorName(PROCESS_TIME));
    eventProcessSensor.add(
        metrics.metricName(PROCESS_TIME_AVG, PROCESS_TIME_AVG_DESC, scope),
        new Avg()
    );
    eventProcessSensor.add(
        metrics.metricName(PROCESS_TIME_MAX, PROCESS_TIME_MAX_DESC, scope),
        new Max()
    );

    schedulingQueueSizeSensor = metrics.addSensor(scope.sensorName(SCHEDULING_QUEUE_SIZE));
    schedulingQueueSizeSensor.add(
        metrics.metricName(SCHEDULING_QUEUE_SIZE, SCHEDULING_QUEUE_SIZE_DESC, scope),
        new Value()
    );

    schedulingQueueLongestQueueSizeSensor
        = metrics.addSensor(scope.sensorName(SCHEDULING_QUEUE_LONGEST_SIZE));
    schedulingQueueLongestQueueSizeSensor.add(
        metrics.metricName(
            SCHEDULING_QUEUE_LONGEST_SIZE,
            SCHEDULING_QUEUE_LONGEST_SIZE_DESC,
            scope
        ),
        new Value()
    );
  }

  public void recordEventProcess(final long durationNanos) {
    eventProcessSensor.record(durationNanos);
  }

  public void recordSchedulingQueueSize(final int size) {
    schedulingQueueSizeSensor.record(size);
  }

  public void recordSchedulingQueueLongestSize(final int size) {
    schedulingQueueLongestQueueSizeSensor.record(size);
  }

  public void recordStateTransition(
      final AsyncEvent.State from,
      final long fromNanos,
      final AsyncEvent.State to,
      final long toNanos
  ) {
    final ResponsiveMetrics.MetricScope innerScope = scope
        .withTags(FROM_STATE, from.name())
        .withTags(TO_STATE, to.name());
    final Sensor sensor = stateTransitionSensors.computeIfAbsent(
        innerScope.sensorName(TRANSITION_TIME),
        sensorName -> {
          final var createdSensor = metrics.addSensor(sensorName);
          createdSensor.add(
              metrics.metricName(TRANSITION_TIME_AVG, TRANSITION_TIME_AVG_DESC, innerScope),
              new Avg()
          );
          createdSensor.add(
              metrics.metricName(TRANSITION_TIME_MAX, TRANSITION_TIME_MAX_DESC, innerScope),
              new Max()
          );
          return createdSensor;
        }
    );
    sensor.record(toNanos - fromNanos);
  }

  public void close() {
    metrics.removeMetric(pendingEventsGauge);
    metrics.removeSensor(eventProcessSensor.name());
    stateTransitionSensors.keySet().forEach(metrics::removeSensor);
  }
}
