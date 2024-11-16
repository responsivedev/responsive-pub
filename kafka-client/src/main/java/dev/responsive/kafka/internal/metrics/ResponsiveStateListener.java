/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.metrics;

import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.APPLICATION_METRIC_GROUP;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.STREAMS_STATE;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.STREAMS_STATE_DESCRIPTION;

import java.io.Closeable;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class ResponsiveStateListener implements StateListener, Closeable {
  private final ResponsiveMetrics metrics;
  private final MetricName stateMetric;

  private StateListener userStateListener;
  private State currentState = State.CREATED;

  public ResponsiveStateListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;

    stateMetric = metrics.metricName(
        STREAMS_STATE,
        STREAMS_STATE_DESCRIPTION,
        metrics.applicationLevelMetric(APPLICATION_METRIC_GROUP)
    );
    metrics.addMetric(stateMetric, (Gauge<Integer>) (config, now) -> currentState.ordinal());
  }

  public void registerUserStateListener(final StateListener userStateListener) {
    this.userStateListener = userStateListener;
  }

  public StateListener userStateListener() {
    return userStateListener;
  }

  @Override
  public void onChange(final State newState, final State oldState) {
    currentState = newState;

    if (userStateListener != null) {
      userStateListener.onChange(newState, oldState);
    }
  }

  @Override
  public void close() {
    metrics.removeMetric(stateMetric);
  }
}
