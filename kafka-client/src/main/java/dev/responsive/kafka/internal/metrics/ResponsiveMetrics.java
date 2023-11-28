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

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
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

  private static final Pattern STREAM_THREAD_REGEX = Pattern.compile(".*-(StreamThread-\\d+)");
  private static final Pattern GLOBAL_THREAD_REGEX = Pattern.compile(".*-(GlobalStreamThread+)");

  private OrderedTagsSupplier orderedTagsSupplier;
  private final OtelMetricsService otelService;
  private final Metrics metrics;

  public ResponsiveMetrics(final Metrics metrics, final OtelMetricsService otelService) {
    this.metrics = metrics;
    this.otelService = otelService;
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

  public interface MetricGroup {

    String groupName();

    // Enforce LinkedHashMap to maintain tag ordering in mbean name
    LinkedHashMap<String, String> tags();
  }

  public MetricGroup applicationLevelMetric() {
    return new ApplicationMetrics(
        orderedTagsSupplier.applicationGroupTags()
    );
  }

  public MetricGroup topicLevelMetric(
      final String threadId,
      final TopicPartition topicPartition
  ) {
    return new TopicMetrics(
        orderedTagsSupplier.topicGroupTags(threadId, topicPartition)
    );
  }

  public MetricGroup storeLevelMetric(
      final String threadId,
      final TopicPartition changelog,
      final String storeName
  ) {
    return new StoreMetrics(
        orderedTagsSupplier.storeGroupTags(threadId, changelog, storeName)
    );
  }

  // Compute/extract the id of this stream thread for any metrics where this information
  // is not already made available
  public String computeThreadId() {
    final String threadName = Thread.currentThread().getName();
    final var streamThreadMatcher = STREAM_THREAD_REGEX.matcher(threadName);
    if (streamThreadMatcher.find()) {
      return streamThreadMatcher.group(1);
    }

    final var globalThreadMatcher = GLOBAL_THREAD_REGEX.matcher(threadName);
    if (globalThreadMatcher.find()) {
      return globalThreadMatcher.group(1);
    }

    LOG.warn("Unable to parse the stream thread id, falling back to thread name {}", threadName);
    return threadName;
  }

  /**
   * @param metricName        a unique name for this metric
   * @param metricDescription a short description of the recorded metric
   * @param metricGroup       the {@link MetricGroup} containing the corresponding group tags
   *
   * @return                  the {@link MetricName}, which should be saved so that this
   *                          metric can be cleaned up when the associated resource is closed
   */
  public MetricName metricName(
      final String metricName,
      final String metricDescription,
      final MetricGroup metricGroup
  ) {
    return new MetricName(
        metricName,
        metricGroup.groupName(),
        metricDescription,
        metricGroup.tags()
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
    otelService.close();
  }
}
