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
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:linelength")
public class ResponsiveMetrics implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveMetrics.class);

  public static final String RESPONSIVE_METRICS_NAMESPACE = "dev.responsive";

  // Base tags that all Responsive metrics are tagged with:
  public static final String RESPONSIVE_VERSION_TAG = "responsive-version";
  public static final String RESPONSIVE_COMMIT_ID_TAG = "responsive-commit-id";
  public static final String STREAMS_VERSION_TAG = "streams-version";
  public static final String STREAMS_COMMIT_ID_TAG = "streams-commit-id";
  public static final String CONSUMER_GROUP_TAG = "consumer-group";
  public static final String STREAMS_APPLICATION_ID_TAG = "streams-application-id";
  public static final String STREAMS_CLIENT_ID_TAG = "streams-client-id";

  // Group tags that are specific to the given metric group and scope
  public static final String THREAD_ID_TAG = "thread-id";
  public static final String TOPIC_TAG = "topic";
  public static final String PARTITION_TAG = "partition";
  public static final String STORE_NAME_TAG = "store-name";

  private final Map<String, String> baseTags = new HashMap<>();
  private final Metrics metrics;

  public ResponsiveMetrics(final Metrics metrics) {
    this.metrics = metrics;
  }

  /**
   * @param applicationId the Streams application id, ie the shared consumer group id
   * @param streamsClientId the Streams client id, ie the process-specific id
   * @param versionMetadata struct containing Responsive and Kafka Streams version info
   * @param userTags optional custom tags that will be attached to each metric
   */
  public void initializeTags(
      final String applicationId,
      final String streamsClientId,
      final ClientVersionMetadata versionMetadata,
      final Map<String, ?> userTags
  ) {
    // this is technically redundant with the application id, but sometimes it's useful to have both
    baseTags.put(CONSUMER_GROUP_TAG, applicationId);
    baseTags.put(STREAMS_APPLICATION_ID_TAG, applicationId);
    baseTags.put(STREAMS_CLIENT_ID_TAG, streamsClientId);

    baseTags.put(RESPONSIVE_VERSION_TAG, versionMetadata.responsiveClientVersion);
    baseTags.put(RESPONSIVE_COMMIT_ID_TAG, versionMetadata.responsiveClientCommitId);
    baseTags.put(STREAMS_VERSION_TAG, versionMetadata.streamsClientVersion);
    baseTags.put(STREAMS_COMMIT_ID_TAG, versionMetadata.streamsClientCommitId);

    for (final var tag : userTags.entrySet()) {
      final String tagKey = tag.getKey();
      final String tagValue = tag.getValue().toString();
      LOG.debug("Adding custom metric tag <{}:{}>", tagKey, tagValue);
      baseTags.put(tagKey, tagValue);
    }
  }

  public interface MetricGroup {

    String groupName();

    Map<String, String> tags();
  }

  public MetricGroup applicationLevelMetric() {
    return new ApplicationMetrics(baseTags);
  }

  public MetricGroup topicLevelMetric(
      final String threadId,
      final TopicPartition topicPartition
  ) {
    return new TopicMetrics(baseTags, threadId, topicPartition);
  }

  public MetricGroup storeLevelMetric(
      final String threadId,
      final String storeName,
      final TopicPartition changelog
  ) {
    return new StoreMetrics(baseTags, threadId, storeName, changelog);
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
  }
}
