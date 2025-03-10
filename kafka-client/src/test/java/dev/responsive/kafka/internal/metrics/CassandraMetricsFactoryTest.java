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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

class CassandraMetricsFactoryTest {

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

      KafkaMetric latencyMetric = assertRegisteredMetric(
          metrics,
          "cql-requests-cumulative-latency"
      );
      long expectedTotalLatency = request1Latency.toMillis() + request2Latency.toMillis();
      assertEquals((double) expectedTotalLatency, latencyMetric.metricValue());
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
      KafkaMetric delayMetric = assertRegisteredMetric(
          metrics,
          "throttling-cumulative-delay"
      );
      long expectedTotalDelay = throttleDelay1.toMillis() + throttleDelay2.toMillis();
      assertEquals((double) expectedTotalDelay, delayMetric.metricValue());
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

  @Test
  public void shouldAggregateAndRecordNodeRequestErrors() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final NodeMetricUpdater nodeMetricUpdater = factory.newNodeUpdater(Mockito.mock(Node.class));

      // When
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, null, 1);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.WRITE_TIMEOUTS, null, 2);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.READ_TIMEOUTS, null, 3);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, null, 4);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.UNAVAILABLES, null, 5);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.ABORTED_REQUESTS, null, 6);
      // Ignore connection errors since they are aggregated separately
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null, 20);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null, 40);

      //Then
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.OTHER_ERRORS, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.WRITE_TIMEOUTS, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.READ_TIMEOUTS, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.UNSENT_REQUESTS, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.UNAVAILABLES, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.ABORTED_REQUESTS, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "cql-request-errors-count");
      assertEquals(21.0, metric.metricValue());
    }
  }

  @Test
  public void shouldAggregateAndRecordNodeConnectionErrors() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final NodeMetricUpdater nodeMetricUpdater = factory.newNodeUpdater(Mockito.mock(Node.class));

      // When
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null, 1);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null, 2);

      // Ignore connection errors since they are aggregated separately
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, null, 10);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.WRITE_TIMEOUTS, null, 20);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.READ_TIMEOUTS, null,  30);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, null, 40);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.UNAVAILABLES, null, 50);
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.ABORTED_REQUESTS, null, 60);

      //Then
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null));
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.AUTHENTICATION_ERRORS, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "connection-errors-count");
      assertEquals(3.0, metric.metricValue());
    }
  }

  @Test
  public void shouldAggregateAndRecordCountOfNodeRetries() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final NodeMetricUpdater nodeMetricUpdater = factory.newNodeUpdater(Mockito.mock(Node.class));

      // When
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.RETRIES, null, 1);
      // The value may be incremented by a number greater than 1
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.RETRIES, null, 5);

      //Then
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.RETRIES, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "cql-request-retries-count");
      assertEquals(6.0, metric.metricValue());
    }
  }

  @Test
  public void shouldAggregateAndRecordCountOfNodeIgnores() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);
      final NodeMetricUpdater nodeMetricUpdater = factory.newNodeUpdater(Mockito.mock(Node.class));

      // When
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.IGNORES, null, 1);
      // The value may be incremented by a number greater than 1
      nodeMetricUpdater.incrementCounter(DefaultNodeMetric.IGNORES, null, 3);

      //Then
      assertTrue(nodeMetricUpdater.isEnabled(DefaultNodeMetric.IGNORES, null));
      KafkaMetric metric = assertRegisteredMetric(metrics, "cql-request-ignores-count");
      assertEquals(4.0, metric.metricValue());
    }
  }

  @Test
  public void shouldUseSameNodeUpdaterForAllNodes() {
    try (Metrics metrics = new Metrics()) {
      // Given
      InternalDriverContext driverContext = mock(InternalDriverContext.class);
      when(driverContext.getMetricRegistry()).thenReturn(new ResponsiveMetrics(metrics));
      final CassandraMetricsFactory factory = new CassandraMetricsFactory(driverContext);

      // When
      Node node1 = Mockito.mock(Node.class);
      when(node1.getHostId()).thenReturn(UUID.randomUUID());
      Node node2 = Mockito.mock(Node.class);
      when(node2.getHostId()).thenReturn(UUID.randomUUID());

      // Then
      assertSame(factory.newNodeUpdater(node1), factory.newNodeUpdater(node2));
    }
  }

  private KafkaMetric assertRegisteredMetric(
      Metrics metrics,
      String name
  ) {
    return metrics.metrics().entrySet().stream()
        .filter(entry -> "cassandra-client".equals(entry.getKey().group())
            && name.equals(entry.getKey().name()))
        .findFirst()
        .orElseThrow(() -> new AssertionFailedError(
            "Failed to find expected metric '" + name + "' in registry."
        ))
        .getValue();
  }

}
