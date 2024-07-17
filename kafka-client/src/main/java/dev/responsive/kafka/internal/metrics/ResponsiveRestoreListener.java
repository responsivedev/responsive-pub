/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.metrics;

import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_INTERRUPTED_CHANGELOGS;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_RESTORING_CHANGELOGS;
import static dev.responsive.kafka.internal.metrics.ApplicationMetrics.NUM_RESTORING_CHANGELOGS_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_RESTORING;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_RESTORING_DESCRIPTION;
import static dev.responsive.kafka.internal.utils.Utils.extractThreadIdFromThreadName;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
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

  private final ConcurrentMap<MetricName, Long> storeMetricToStartMs = new ConcurrentHashMap<>();

  private StateRestoreListener userRestoreListener;

  private MetricName timeRestoringMetricName(
      final TopicPartition topicPartition,
      final String storeName
  ) {
    return metrics.metricName(
        TIME_RESTORING,
        TIME_RESTORING_DESCRIPTION,
        metrics.storeLevelMetric(
            StoreMetrics.STORE_METRIC_GROUP,
            extractThreadIdFromThreadName(Thread.currentThread().getName()),
            topicPartition,
            storeName
        )
    );
  }

  public ResponsiveRestoreListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;

    numRestoringMetricName = metrics.metricName(
        NUM_RESTORING_CHANGELOGS,
        NUM_RESTORING_CHANGELOGS_DESCRIPTION,
        metrics.applicationLevelMetric(ApplicationMetrics.APPLICATION_METRIC_GROUP)
    );

    metrics.addMetric(
        numRestoringMetricName,
        (Gauge<Integer>) (config, now) -> storeMetricToStartMs.size()
    );

    numInterruptedSensor = metrics.addSensor(NUM_INTERRUPTED_CHANGELOGS);
    numInterruptedSensor.add(
        metrics.metricName(
            NUM_INTERRUPTED_CHANGELOGS,
            NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION,
            metrics.applicationLevelMetric(ApplicationMetrics.APPLICATION_METRIC_GROUP)),
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
  public void onRestoreStart(
      final TopicPartition topicPartition,
      final String storeName,
      final long startingOffset,
      final long endingOffset
  ) {
    final long startMs = System.currentTimeMillis();
    final MetricName timeRestoringMetric = timeRestoringMetricName(topicPartition, storeName);
    storeMetricToStartMs.put(timeRestoringMetric, startMs);
    metrics.addMetric(timeRestoringMetric, (Gauge<Long>) (config, now) -> now - startMs);

    LOG.info("Beginning restoration from offset {} to {} at {}ms (epoch time) for partition {} "
                 + "of state store {}",
             startingOffset, endingOffset, startMs, topicPartition, storeName);

    if (userRestoreListener != null) {
      userRestoreListener.onRestoreStart(
          topicPartition,
          storeName,
          startingOffset,
          endingOffset);
    }
  }

  // NOTE: we'll need to synchronize this if we ever do anything more than log and delegate
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
    final MetricName timeRestoringMetric = timeRestoringMetricName(topicPartition, storeName);

    final long currentTimeMs = System.currentTimeMillis();
    final long restoreTime = removeStateStore(timeRestoringMetric, currentTimeMs);

    LOG.info("Finished restoration of {} total records after {}ms for partition {} "
                 + "of state store {}", totalRestored, restoreTime, topicPartition, storeName);

    if (userRestoreListener != null) {
      userRestoreListener.onRestoreEnd(
          topicPartition,
          storeName,
          totalRestored);
    }
  }

  // TODO(3.5+): uncomment out the override annotation once we upgrade to 3.5 or above.
  //  Note however that due to KAFKA-15571 this new callback is broken and therefore still
  //  needs to be manually invoked by us from #onStoreClosed until/unless we can upgrade to
  //  3.5.2, 3.6.1, or 3.7.0
  //@Override
  public void onRestoreSuspended(
      final TopicPartition topicPartition,
      final String storeName,
      final long totalRestored
  ) {
    final MetricName timeRestoringMetric = timeRestoringMetricName(topicPartition, storeName);

    final long currentTimeMs = System.currentTimeMillis();
    final long restoreTime = removeStateStore(timeRestoringMetric, currentTimeMs);

    numInterruptedSensor.record(1, currentTimeMs);

    LOG.info("Suspended restoration of {} total records after {}ms for partition {} "
                 + "of state store {}", totalRestored, restoreTime, topicPartition, storeName);

    /* TODO: uncomment when we upgrade to a version that has the callback (3.5+ is fine)
    if (userRestoreListener != null) {
      userRestoreListener.onRestoreSuspended(
          topicPartition,
          storeName,
          totalRestored
      );
    }
     */
  }

  // TODO: remove this once we upgrade to a version where the #onRestoreSuspended callback exists
  //  and will be invoked by Kafka Streams directly (ie includes fix for KAFKA-15571)
  public void onStoreClosed(
      final TopicPartition topicPartition,
      final String storeName
  ) {
    final MetricName timeRestoringMetric = timeRestoringMetricName(topicPartition, storeName);

    if (storeMetricToStartMs.containsKey(timeRestoringMetric)) {
      onRestoreSuspended(topicPartition, storeName, -1L); // just pass in a dummy value for now
    }
  }

  /**
   * Clean up the state store and any metrics associated with it
   *
   * @return the amount of time spent restoring this state store
   */
  private long removeStateStore(final MetricName timeRestoringMetric, final long currentTimeMs) {
    metrics.removeMetric(timeRestoringMetric);

    final long restoreStartMs = storeMetricToStartMs.remove(timeRestoringMetric);
    return currentTimeMs - restoreStartMs;
  }

  @Override
  public void close() {
    if (!storeMetricToStartMs.isEmpty()) {
      LOG.warn("Not all state stores being restored were closed before ending shutdown");
    }
    metrics.removeMetric(numRestoringMetricName);
    metrics.removeSensor(NUM_INTERRUPTED_CHANGELOGS);
  }

}