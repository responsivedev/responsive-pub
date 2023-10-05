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

    stateMetric = metrics.metricName(
        STREAMS_STATE,
        STREAMS_STATE_DESCRIPTION,
        metrics.applicationLevelMetric()
    );
    metrics.addMetric(stateMetric, (config, now) -> currentState.ordinal());
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
