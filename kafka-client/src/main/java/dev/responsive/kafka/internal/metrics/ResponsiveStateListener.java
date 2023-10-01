package dev.responsive.kafka.internal.metrics;

import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.STREAMS_STATE;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.STREAMS_STATE_DESCRIPTION;

import java.io.Closeable;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class ResponsiveStateListener implements StateListener, Closeable {
  private final ResponsiveMetrics metrics;
  private final MetricName stateMetric;

  private StateListener userStateListener;
  private State currentState = State.CREATED;

  public ResponsiveStateListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;
    this.stateMetric = metrics.metricName(
        STREAMS_STATE,
        STREAMS_STATE_DESCRIPTION,
        metrics.applicationLevelMetric(),
        (config, now) -> currentState.ordinal()
    );
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
