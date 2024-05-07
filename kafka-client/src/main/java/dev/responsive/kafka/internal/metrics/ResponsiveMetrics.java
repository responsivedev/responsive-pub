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

import dev.responsive.kafka.internal.metrics.exporter.MetricsExportService;
import dev.responsive.kafka.internal.metrics.exporter.NoopMetricsExporterService;
import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveMetrics implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveMetrics.class);

  public static final String RESPONSIVE_METRICS_NAMESPACE = "dev.responsive";

  public static final String AVG_SUFFIX = "-avg";
  public static final String MAX_SUFFIX = "-max";
  public static final String RATE_SUFFIX = "-rate";
  public static final String TOTAL_SUFFIX = "-total";

  public static final String AVG_DESCRIPTION = "The average ";
  public static final String MAX_DESCRIPTION = "The maximum ";
  public static final String RATE_DESCRIPTION = "The rate of ";
  public static final String TOTAL_DESCRIPTION = "The total ";

  private OrderedTagsSupplier orderedTagsSupplier;
  private final MetricsExportService exportService;
  private final Metrics metrics;

  public ResponsiveMetrics(final Metrics metrics) {
    this(metrics, new NoopMetricsExporterService());
  }

  public ResponsiveMetrics(final Metrics metrics, final MetricsExportService exportService) {
    this.metrics = metrics;
    this.exportService = exportService;
  }

  /**
   * @param applicationId the Streams application id, ie the shared consumer group id
   * @param streamsClientId the Streams client id, ie the process-specific id
   * @param versionMetadata struct containing Responsive and Kafka Streams version info
   * @param userTags optional custom tags that will be attached to each metric in alphabetic order
   */
  public void initializeTags(
      final String applicationId,
      final String streamsClientId,
      final ClientVersionMetadata versionMetadata,
      final Map<String, ?> userTags
  ) {
    orderedTagsSupplier = new OrderedTagsSupplier(
        versionMetadata.responsiveClientVersion,
        versionMetadata.responsiveClientCommitId,
        versionMetadata.streamsClientVersion,
        versionMetadata.streamsClientCommitId,
        applicationId, // consumer group is technically redundant with app-id, but both are useful
        applicationId,
        streamsClientId,
        userTags
    );
  }

  public static class MetricScope {
    private final String groupName;
    private final LinkedHashMap<String, String> tags;

    // make this private so that users have to create one of the scopes defined below
    private MetricScope(final String groupName, final LinkedHashMap<String, String> tags) {
      this.groupName = groupName;
      this.tags = tags;
    }

    public String groupName() {
      return groupName;
    }

    // Enforce LinkedHashMap to maintain tag ordering in mbean name
    public LinkedHashMap<String, String> tags() {
      return tags;
    }

    public String sensorName(final String name) {
      return String.join("/",
          groupName(),
          name,
          tags().entrySet().stream()
              .map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(","))
      );
    }

    public MetricScope withTags(final String key, final String val) {
      final LinkedHashMap<String, String> updated = new LinkedHashMap<>(tags);
      if (tags.containsKey(key)) {
        throw new IllegalStateException("Duplicate tag key " + key);
      }
      updated.put(key, val);
      return new MetricScope(groupName, updated);
    }
  }

  public MetricScope applicationLevelMetric(final String group) {
    return new MetricScope(
        group,
        orderedTagsSupplier.applicationGroupTags()
    );
  }

  public MetricScope processorLevelMetric(
      final String group,
      final String threadId,
      final TaskId taskId,
      final String processorName
  ) {
    return new MetricScope(
        group,
        orderedTagsSupplier.processorGroupTags(threadId, taskId, processorName));
  }

  public MetricScope threadLevelMetric(final String group, final String threadId) {
    return new MetricScope(
        group,
        orderedTagsSupplier.threadGroupTags(threadId));
  }

  public MetricScope topicLevelMetric(
      final String group,
      final String threadId,
      final TopicPartition topicPartition
  ) {
    return new MetricScope(
        group,
        orderedTagsSupplier.topicGroupTags(threadId, topicPartition)
    );
  }

  public MetricScope storeLevelMetric(
      final String group,
      final String threadId,
      final TopicPartition changelog,
      final String storeName
  ) {
    return new MetricScope(
        group,
        orderedTagsSupplier.storeGroupTags(threadId, changelog, storeName)
    );
  }

  /**
   * @param metricName        a unique name for this metric
   * @param metricDescription a short description of the recorded metric
   * @param metricScope       the {@link MetricScope} containing the corresponding group tags
   *
   * @return                  the {@link MetricName}, which should be saved so that this
   *                          metric can be cleaned up when the associated resource is closed
   */
  public MetricName metricName(
      final String metricName,
      final String metricDescription,
      final MetricScope metricScope
  ) {
    return new MetricName(
        metricName,
        metricScope.groupName(),
        metricDescription,
        metricScope.tags()
    );
  }

  /**
   * @param metricName        a unique name for this metric
   * @param metricDescription a short description of the recorded metric
   * @param metricScope       the {@link MetricScope} containing the corresponding group tags
   * @param additionalTags    a set of additional tags to be attached to the metric
   *
   * @return                  the {@link MetricName}, which should be saved so that this
   *                          metric can be cleaned up when the associated resource is closed
   */
  public MetricName metricName(
      final String metricName,
      final String metricDescription,
      final MetricScope metricScope,
      final LinkedHashMap<String, String> additionalTags
  ) {
    final LinkedHashMap<String, String> allTags = new LinkedHashMap<>(metricScope.tags());
    allTags.putAll(additionalTags);
    return new MetricName(
        metricName,
        metricScope.groupName(),
        metricDescription,
        allTags
    );
  }

  public void addMetric(final MetricName metricName, final MetricValueProvider<?> valueProvider) {
    metrics.addMetric(metricName, valueProvider);
  }

  public Sensor addSensor(final String sensorName) {
    return metrics.sensor(sensorName);
  }

  public void removeSensor(final String sensorName) {
    metrics.removeSensor(sensorName);
  }

  public void removeMetric(final MetricName metricName) {
    metrics.removeMetric(metricName);
  }

  public Map<MetricName, KafkaMetric> metrics() {
    return metrics.metrics();
  }

  @Override
  public void close() {
    if (!metrics().isEmpty()) {
      LOG.warn("Not all metrics were cleaned up before close: {}", metrics().keySet());
    }
    metrics.close();
    exportService.close();
  }
}
