package dev.responsive.internal.metrics;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
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

  /**
   * @param applicationId the Streams application id, ie the shared consumer group id
   * @param streamsClientId the Streams client id, ie the process-specific id
   */
  public ResponsiveStateListener(
      final Metrics metrics,
      final String applicationId,
      final String streamsClientId
  ) {
    this.metrics = metrics;
    final Map<String, String> tags = new HashMap<>();
    tags.put("application-id", applicationId);
    tags.put("client-id", streamsClientId);

    stateMetricName = new MetricName(
        "state",
        "stream-application-metrics",
        "The current KafkaStreams.State expressed as its ordinal value",
        tags
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

    if (userStateListener != null) {
      userStateListener.onChange(newState, oldState);
    }
  }

  private long currentStateId() {
    return currentState.ordinal();
  }

  @Override
  public void close() {
    metrics.removeMetric(stateMetricName);
  }
}
