package dev.responsive.kafka.store;

import java.io.Closeable;
import java.util.Collections;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class ResponsiveStateListener implements StateListener, Closeable {
  private final Metrics metrics;
  private final MetricName stateMetricName;

  private StateListener userStateListener;
  private State currentState;

  public ResponsiveStateListener(final Metrics metrics, final String applicationId) {
    this.metrics = metrics;
    stateMetricName = new MetricName(
        "state",
        "stream-application-metrics",
        "The current KafkaStreams.State expressed as its ordinal value",
        Collections.singletonMap("application", applicationId)
    );

    metrics.addMetric(stateMetricName, (Gauge<Long>) (config, now) -> currentStateId());
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
    userStateListener.onChange(newState, oldState);
  }

  private long currentStateId() {
    return currentState.ordinal();
  }

  @Override
  public void close() {
    metrics.removeMetric(stateMetricName);
  }
}
