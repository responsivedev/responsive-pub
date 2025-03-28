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

package dev.responsive.kafka.internal.db.mongo;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

/**
 * Listener which reports MongoDB command failures and successes. For each result,
 * we record a total count representing the number of commands executed which had that
 * result (success or failure), and a cumulative latency. Reported metrics are tagged by
 * the MongoDB command name.
 */
public class MongoTelemetryListener implements CommandListener {
  private static final String MONGODB_METRICS_GROUP = "mongodb-client";

  static final MetricName COMMANDS_SUCCEEDED_LATENCY = new MetricName(
      "commands-succeeded-cumulative-latency",
      MONGODB_METRICS_GROUP,
      "cumulative commands succeeded latency",
      Collections.emptyMap()
  );
  private static final MetricName COMMANDS_SUCCEEDED_COUNT = new MetricName(
      "commands-succeeded-count",
      MONGODB_METRICS_GROUP,
      "total commands succeeded",
      Collections.emptyMap()
  );

  static final MetricName COMMANDS_FAILED_COUNT = new MetricName(
        "commands-failed-count",
        MONGODB_METRICS_GROUP,
        "total commands failed",
        Collections.emptyMap()
    );
  static final MetricName COMMANDS_FAILED_LATENCY = new MetricName(
      "commands-failed-cumulative-latency",
      MONGODB_METRICS_GROUP,
      "cumulative commands failed latency",
      Collections.emptyMap()
  );

  private final ResponsiveMetrics metrics;

  public MongoTelemetryListener(ResponsiveMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public void commandSucceeded(final CommandSucceededEvent event) {
    getOrCreateSensor(event.getCommandName(), true)
        .record(event.getElapsedTime(TimeUnit.MILLISECONDS));
  }

  @Override
  public void commandFailed(final CommandFailedEvent event) {
    getOrCreateSensor(event.getCommandName(), false)
        .record(event.getElapsedTime(TimeUnit.MILLISECONDS));
  }

  private Sensor getOrCreateSensor(
      String command,
      boolean isSuccess
  ) {
    String sensorName = "mongodb-commands" + "-" + command
        + (isSuccess ? "-succeeded" : "-failed");

    Sensor sensor = metrics.getSensor(sensorName);
    if (sensor == null) {
      sensor = metrics.addSensor(sensorName);

      if (isSuccess) {
        sensor.add(commandsSucceededCount(command), new CumulativeCount());
        sensor.add(commandsSucceededLatency(command), new CumulativeSum());
      } else {
        sensor.add(commandsFailedCount(command), new CumulativeCount());
        sensor.add(commandsFailedLatency(command), new CumulativeSum());
      }
    }

    return sensor;
  }

  static MetricName commandsSucceededCount(String command) {
    return metricName(COMMANDS_SUCCEEDED_COUNT, buildTags(command));
  }

  static MetricName commandsSucceededLatency(String command) {
    return metricName(COMMANDS_SUCCEEDED_LATENCY, buildTags(command));
  }

  static MetricName commandsFailedCount(String command) {
    return metricName(COMMANDS_FAILED_COUNT, buildTags(command));
  }

  static MetricName commandsFailedLatency(String command) {
    return metricName(COMMANDS_FAILED_LATENCY, buildTags(command));
  }

  private static Map<String, String> buildTags(String command) {
    return Collections.singletonMap("command", command);
  }

  private static MetricName metricName(
      MetricName template,
      Map<String, String> tags
  ) {
    return new MetricName(
        template.name(),
        template.group(),
        template.description(),
        tags
    );
  }

}
