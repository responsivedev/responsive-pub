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

import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.CLIENT_METRIC_GROUP;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.NUM_INTERRUPTED;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.NUM_INTERRUPTED_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.NUM_RESTORING;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.NUM_RESTORING_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESTORE_LATENCY;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESTORE_LATENCY_AVG;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESTORE_LATENCY_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.RESTORE_LATENCY_MAX;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.STORE_METRIC_GROUP;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.streamsClientMetricGroupTags;
import static dev.responsive.kafka.internal.metrics.ResponsiveMetrics.storeMetricGroupTags;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: this is a global object which is shared across all StreamThreads and state stores in
 * this Streams application.
 */
public class ResponsiveRestoreListener implements StateRestoreListener, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveRestoreListener.class);

  private final ResponsiveMetrics metrics;
  private final MetricName numRestoringMetricName;
  private final Sensor numInterruptedSensor;
  private final Sensor restoreLatencySensor;

  private final Map<String, String> baseTags = new HashMap<>();

  private final Map<TopicPartition, Long> restoringChangelogToStartMs = new HashMap<>();
  private final Map<TopicPartition, MetricName> changelogToMetricName = new HashMap<>();

  private StateRestoreListener userRestoreListener;

  public ResponsiveRestoreListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;
    this.numRestoringMetricName = new MetricName(
        NUM_RESTORING,
        CLIENT_METRIC_GROUP,
        NUM_RESTORING_DESCRIPTION,
        baseTags
    );
    this.numInterruptedSensor = metrics.sensor(NUM_INTERRUPTED);
    this.restoreLatencySensor = metrics.sensor(RESTORE_LATENCY);
  }

  public void initializeRestoreMetrics(final String applicationId, final String streamsClientId) {
    baseTags.putAll(streamsClientMetricGroupTags(applicationId, streamsClientId));

    metrics.addMetric(
        numRestoringMetricName,
        (Gauge<Integer>) (config, now) -> restoringChangelogToStartMs.size()
    );

    numInterruptedSensor.add(
        new MetricName(
            NUM_INTERRUPTED,
            CLIENT_METRIC_GROUP,
            NUM_INTERRUPTED_DESCRIPTION,
            baseTags),
        new CumulativeCount()
    );
  }

  public void registerUserRestoreListener(final StateRestoreListener restoreListener) {
    userRestoreListener = restoreListener;
  }

  public StateRestoreListener userRestoreListener() {
    return userRestoreListener;
  }

  @Override
  public synchronized void onRestoreStart(
      final TopicPartition topicPartition,
      final String storeName,
      final long startingOffset,
      final long endingOffset
  ) {
    LOG.info("Beginning restoration from offset {} to {} for partition {} of state store {}",
             startingOffset, endingOffset, topicPartition, storeName);

    restoringChangelogToStartMs.put(topicPartition, System.currentTimeMillis());
    addStoreRestoreSensor(topicPartition);

    if (userRestoreListener != null) {
      userRestoreListener.onRestoreStart(
          topicPartition,
          storeName,
          startingOffset,
          endingOffset);
    }
  }

  @Override
  public void onBatchRestored(
      final TopicPartition topicPartition,
      final String storeName,
      final long batchEndOffset,
      final long numRestored
  ) {
    LOG.debug("Restored {} more records up to offset {} for partition {} of state store {}",
              numRestored, batchEndOffset, topicPartition, storeName);

    if (userRestoreListener != null) {
      userRestoreListener.onBatchRestored(
          topicPartition,
          storeName,
          batchEndOffset,
          numRestored);
    }
  }

  @Override
  public void onRestoreEnd(
      final TopicPartition topicPartition,
      final String storeName,
      final long totalRestored
  ) {
    LOG.info("Finished restoration of {} total records for partition {} of state store {}",
             totalRestored, topicPartition, storeName);

    restoringChangelogToStartMs.remove(topicPartition);
    restoreLatencySensor.

    if (userRestoreListener != null) {
      userRestoreListener.onRestoreEnd(
          topicPartition,
          storeName,
          totalRestored);
    }
  }

  /**
   * Deregister a store and clean up any associated metrics, for example when shutting down or
   * preparing to migrate an active task to a different node.
   */
  // TODO: in 3.5 another callback was added to the restore listener: #onRestoreSuspend
  //   *  We can replace this method with #onRestoreSuspend (in combination with #onRestoreEnd)
  //   *  to ensure that all metrics and tracked stores are properly cleaned up when they leave
  //   *  this node for any reason. This is not possible without #onRestoreSuspend, as the restore
  //   *  listener won't always be notified that a store has stopped the restoration process:
  //   *  for example because the task/store is being migrated elsewhere, shut down, or otherwise
  //   *  interrupted mid-restoration. #onRestoreEnd is only invoked when a task has finished
  //   *  restoration of the entire changelog and as such, cannot be relied on for cleanup.
  public void onRestoreInterrupted(final TopicPartition topicPartition) {
    restoringChangelogToStartMs.remove(topicPartition);
    numInterruptedSensor.record();
  }

  public void addStoreRestoreSensor(final TopicPartition topicPartition) {
    restoreLatencySensor.add(
        metricName(RESTORE_LATENCY_AVG, "The average " + RESTORE_LATENCY_DESCRIPTION, topicPartition),
        new Avg()
    );
    restoreLatencySensor.add(
        metricName(RESTORE_LATENCY_MAX, "The maximum " + RESTORE_LATENCY_DESCRIPTION, topicPartition),
        new Max()
    );
  }


  @Override
  public void close() {
    metrics.removeMetric(numRestoringMetricName);
    metrics.removeSensor(NUM_INTERRUPTED);
    metrics.removeSensor(RESTORE_LATENCY);
  }

  private MetricName metricName(
      final String metricName,
      final String metricDescription,
      final TopicPartition topicPartition
  ) {
    return new MetricName(
        metricName,
        STORE_METRIC_GROUP,
        metricDescription,
        storeMetricGroupTags(topicPartition, baseTags)
    );
  }

}
