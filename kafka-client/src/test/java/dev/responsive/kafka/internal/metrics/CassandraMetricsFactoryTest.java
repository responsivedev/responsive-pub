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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class CassandraMetricsSeFactoryTest {

  @Test
  public void shouldRecordCountOfBytesSent() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      sessionUpdater.markMeter(DefaultSessionMetric.BYTES_SENT, null, 100);
      sessionUpdater.markMeter(DefaultSessionMetric.BYTES_SENT, null, 300);

      // Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.BYTES_SENT, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "bytes-sent");
      assertEquals(400.0, metric.metricValue());
    }
  }

  @Test
  public void shouldRecordCountOfBytesReceived() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      sessionUpdater.markMeter(DefaultSessionMetric.BYTES_RECEIVED, null, 300);
      sessionUpdater.markMeter(DefaultSessionMetric.BYTES_RECEIVED, null, 200);

      // Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.BYTES_RECEIVED, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "bytes-received");
      assertEquals(500.0, metric.metricValue());
    }
  }

  @Test
  public void shouldRecordCountAndCumulativeLatencyOfCqlRequests() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      // Report metrics in nanoseconds to ensure proper conversion to milliseconds
      Duration request1Latency = Duration.ofMillis(500);
      sessionUpdater.updateTimer(
          DefaultSessionMetric.CQL_REQUESTS,
          null,
          request1Latency.toNanos(),
          TimeUnit.NANOSECONDS
      );
      // Measurement is not sensitive to the reported unit
      Duration request2Latency = Duration.ofMillis(100);
      sessionUpdater.updateTimer(
          DefaultSessionMetric.CQL_REQUESTS,
          null,
          request2Latency.toMillis(),
          TimeUnit.MILLISECONDS
      );


      // Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.CQL_REQUESTS, null));

      KafkaMetric countMetric = assertRegisteredMetric(metrics, "cql-requests-count");
      assertEquals((double) 2, countMetric.metricValue());

      KafkaMetric latencyMetric = assertRegisteredMetric(metrics, "cql-requests-cumulative-latency");
      assertEquals((double) (request1Latency.toMillis() + request2Latency.toMillis()), latencyMetric.metricValue());
    }
  }

  @Test
  public void shouldRecordCountOfClientRequestTimeouts() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      sessionUpdater.incrementCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, null, 1);
      // The value may be incremented by a number greater than 1
      sessionUpdater.incrementCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, null, 5);

      // Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "cql-request-timeouts-count");
      assertEquals(6.0, metric.metricValue());
    }
  }

  @Test
  public void shouldRecordCumulativeThrottlingDelay() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      // Report metrics in nanoseconds to ensure proper conversion to milliseconds
      Duration throttleDelay1 = Duration.ofMillis(500);
      sessionUpdater.updateTimer(
          DefaultSessionMetric.THROTTLING_DELAY,
          null,
          throttleDelay1.toNanos(),
          TimeUnit.NANOSECONDS
      );
      // Measurement is not sensitive to the reported unit
      Duration throttleDelay2 = Duration.ofMillis(100);
      sessionUpdater.updateTimer(
          DefaultSessionMetric.THROTTLING_DELAY,
          null,
          throttleDelay2.toMillis(),
          TimeUnit.MILLISECONDS
      );

      // Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.THROTTLING_DELAY, null));
      KafkaMetric delayMetric = assertRegisteredMetric(metrics, "throttling-cumulative-delay");
      assertEquals((double) (throttleDelay1.toMillis() + throttleDelay2.toMillis()), delayMetric.metricValue());
    }
  }

  @Test
  public void shouldRecordCountOfThrottlingErrors() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final SessionMetricUpdater sessionUpdater = factory.getSessionUpdater();

      // When
      sessionUpdater.incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, null, 1);
      // The value may be incremented by a number greater than 1
      sessionUpdater.incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, null, 10);

      //Then
      assertTrue(sessionUpdater.isEnabled(DefaultSessionMetric.THROTTLING_ERRORS, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "throttling-errors-count");
      assertEquals(11.0, metric.metricValue());
    }
  }

  private KafkaMetric assertRegisteredMetric(
      Metrics metrics,
      String name
  ) {
    return metrics.metrics().entrySet().stream()
        .filter(entry -> "cassandra-driver".equals(entry.getKey().group()) && name.equals(entry.getKey().name()))
        .findFirst()
        .orElseThrow(() -> new AssertionFailedError("Failed to find expected metric '" + name + "' in registry."))
        .getValue();
  }

}
