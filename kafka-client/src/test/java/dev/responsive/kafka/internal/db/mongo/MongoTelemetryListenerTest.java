/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.db.mongo;

import static dev.responsive.kafka.internal.db.mongo.MongoTelemetryListener.commandsFailedCount;
import static dev.responsive.kafka.internal.db.mongo.MongoTelemetryListener.commandsFailedLatency;
import static dev.responsive.kafka.internal.db.mongo.MongoTelemetryListener.commandsSucceededCount;
import static dev.responsive.kafka.internal.db.mongo.MongoTelemetryListener.commandsSucceededLatency;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandSucceededEvent;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.exporter.MetricsExportService;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MongoTelemetryListenerTest {

  @Test
  public void shouldHaveCorrectNamesForSuccessMetrics() {
    final MetricName findSuccessCountName = commandsSucceededCount("find");
    assertEquals("mongodb-client", findSuccessCountName.group());
    assertEquals("commands-succeeded-count", findSuccessCountName.name());
    assertEquals(Collections.singletonMap("command", "find"), findSuccessCountName.tags());

    final MetricName findSuccessLatencyName = commandsSucceededLatency("find");
    assertEquals("mongodb-client", findSuccessLatencyName.group());
    assertEquals("commands-succeeded-cumulative-latency", findSuccessLatencyName.name());
    assertEquals(Collections.singletonMap("command", "find"), findSuccessLatencyName.tags());
  }

  @Test
  public void shouldHaveCorrectNamesForFailureMetrics() {
    final MetricName findFailureCountName = commandsFailedCount("find");
    assertEquals("mongodb-client", findFailureCountName.group());
    assertEquals("commands-failed-count", findFailureCountName.name());
    assertEquals(Collections.singletonMap("command", "find"), findFailureCountName.tags());

    final MetricName findFailureLatencyName = commandsFailedLatency("find");
    assertEquals("mongodb-client", findFailureLatencyName.group());
    assertEquals("commands-failed-cumulative-latency", findFailureLatencyName.name());
    assertEquals(Collections.singletonMap("command", "find"), findFailureLatencyName.tags());
  }

  @Test
  public void shouldRecordSuccessfulCommandExecution() {
    final ResponsiveMetrics metrics = new ResponsiveMetrics(
        new Metrics(),
        Mockito.mock(MetricsExportService.class)
    );

    final MongoTelemetryListener listener = new MongoTelemetryListener(metrics);
    final CommandSucceededEvent successEvent = Mockito.mock(CommandSucceededEvent.class);

    Mockito.when(successEvent.getCommandName()).thenReturn("count");
    int initialMetricsCount = metrics.metrics().size();
    Mockito.when(successEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(200L);
    listener.commandSucceeded(successEvent);

    assertEquals(initialMetricsCount + 2, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsSucceededCount("count")));
    assertEquals(200L, metricValue(metrics, commandsSucceededLatency("count")));

    Mockito.when(successEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(300L);
    listener.commandSucceeded(successEvent);
    assertEquals(2L, metricValue(metrics, commandsSucceededCount("count")));
    assertEquals(500L, metricValue(metrics, commandsSucceededLatency("count")));

    Mockito.when(successEvent.getCommandName()).thenReturn("find");
    listener.commandSucceeded(successEvent);
    assertEquals(initialMetricsCount + 4, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsSucceededCount("find")));
    assertEquals(300L, metricValue(metrics, commandsSucceededLatency("find")));
    assertEquals(2L, metricValue(metrics, commandsSucceededCount("count")));
    assertEquals(500L, metricValue(metrics, commandsSucceededLatency("count")));

    final CommandFailedEvent failedEvent = Mockito.mock(CommandFailedEvent.class);
    Mockito.when(failedEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(50L);
    Mockito.when(failedEvent.getCommandName()).thenReturn("find");
    listener.commandFailed(failedEvent);
    assertEquals(initialMetricsCount + 6, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsFailedCount("find")));
    assertEquals(50L, metricValue(metrics, commandsFailedLatency("find")));
    assertEquals(1L, metricValue(metrics, commandsSucceededCount("find")));
    assertEquals(300L, metricValue(metrics, commandsSucceededLatency("find")));
    assertEquals(2L, metricValue(metrics, commandsSucceededCount("count")));
    assertEquals(500L, metricValue(metrics, commandsSucceededLatency("count")));
  }

  @Test
  public void shouldRecordFailedCommandExecution() {
    final ResponsiveMetrics metrics = new ResponsiveMetrics(
        new Metrics(),
        Mockito.mock(MetricsExportService.class)
    );

    final MongoTelemetryListener listener = new MongoTelemetryListener(metrics);
    final CommandFailedEvent failedEvent = Mockito.mock(CommandFailedEvent.class);

    Mockito.when(failedEvent.getCommandName()).thenReturn("count");
    int initialMetricsCount = metrics.metrics().size();
    Mockito.when(failedEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(200L);
    listener.commandFailed(failedEvent);

    assertEquals(initialMetricsCount + 2, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsFailedCount("count")));
    assertEquals(200L, metricValue(metrics, commandsFailedLatency("count")));

    Mockito.when(failedEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(300L);
    listener.commandFailed(failedEvent);
    assertEquals(2L, metricValue(metrics, commandsFailedCount("count")));
    assertEquals(500L, metricValue(metrics, commandsFailedLatency("count")));

    Mockito.when(failedEvent.getCommandName()).thenReturn("find");
    listener.commandFailed(failedEvent);
    assertEquals(initialMetricsCount + 4, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsFailedCount("find")));
    assertEquals(300L, metricValue(metrics, commandsFailedLatency("find")));
    assertEquals(2L, metricValue(metrics, commandsFailedCount("count")));
    assertEquals(500L, metricValue(metrics, commandsFailedLatency("count")));

    final CommandSucceededEvent successEvent = Mockito.mock(CommandSucceededEvent.class);
    Mockito.when(successEvent.getCommandName()).thenReturn("count");
    Mockito.when(successEvent.getElapsedTime(TimeUnit.MILLISECONDS)).thenReturn(50L);
    listener.commandSucceeded(successEvent);
    assertEquals(initialMetricsCount + 6, metrics.metrics().size());
    assertEquals(1L, metricValue(metrics, commandsSucceededCount("count")));
    assertEquals(50L, metricValue(metrics, commandsSucceededLatency("count")));
    assertEquals(1L, metricValue(metrics, commandsFailedCount("find")));
    assertEquals(300L, metricValue(metrics, commandsFailedLatency("find")));
    assertEquals(2L, metricValue(metrics, commandsFailedCount("count")));
    assertEquals(500L, metricValue(metrics, commandsFailedLatency("count")));
  }

  private long metricValue(
      ResponsiveMetrics metrics,
      MetricName name
  ) {
    Double value = (Double) metrics.metrics()
        .get(name)
        .metricValue();
    return value.longValue();
  }

}