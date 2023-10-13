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
import static dev.responsive.kafka.internal.metrics.StoreMetrics.RESTORE_LATENCY;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.RESTORE_LATENCY_AVG;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.RESTORE_LATENCY_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.RESTORE_LATENCY_MAX;

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

  private final Map<TopicPartition, Long> restoringChangelogToStartMs = new HashMap<>();

  private StateRestoreListener userRestoreListener;

  public ResponsiveRestoreListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;

    numRestoringMetricName = metrics.metricName(
        NUM_RESTORING_CHANGELOGS,
        NUM_RESTORING_CHANGELOGS_DESCRIPTION,
        metrics.applicationLevelMetric()
    );
    numInterruptedSensor = metrics.addSensor(NUM_INTERRUPTED_CHANGELOGS);
    restoreLatencySensor = metrics.addSensor(RESTORE_LATENCY);

    metrics.addMetric(
        numRestoringMetricName,
        (Gauge<Integer>) (config, now) -> restoringChangelogToStartMs.size()
    );

    numInterruptedSensor.add(
        metrics.metricName(
            NUM_INTERRUPTED_CHANGELOGS,
            NUM_INTERRUPTED_CHANGELOGS_DESCRIPTION,
            metrics.applicationLevelMetric()),
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
    final long startMs = System.currentTimeMillis();
    restoringChangelogToStartMs.put(topicPartition, startMs);

    restoreLatencySensor.add(
        metrics.metricName(
            RESTORE_LATENCY_AVG,
            "The average " + RESTORE_LATENCY_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), topicPartition, storeName)),
        new Avg()
    );
    restoreLatencySensor.add(
        metrics.metricName(
            RESTORE_LATENCY_MAX,
            "The maximum " + RESTORE_LATENCY_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), topicPartition, storeName)),
        new Max()
    );

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
    final long restoreTime = recordRestorationEnded(topicPartition);

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
    numInterruptedSensor.record();
    final long restoreTime = recordRestorationEnded(topicPartition);

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

  private long recordRestorationEnded(final TopicPartition topicPartition) {
    final long startMs = restoringChangelogToStartMs.remove(topicPartition);
    final long endMs = System.currentTimeMillis();
    final long restoreLatency = endMs - startMs;
    restoreLatencySensor.record(restoreLatency, endMs);
    return restoreLatency;
  }

  public void onStoreClosed(
      final TopicPartition topicPartition,
      final String storeName
  ) {
    // TODO: remove this once we upgrade to a version where this callback exists and will
    //  be invoked by Kafka Streams directly (ie includes fix for KAFKA-15571)
    if (restoringChangelogToStartMs.containsKey(topicPartition)) {
      onRestoreSuspended(topicPartition, storeName, -1L); // just pass in a dummy value for now
    }
  }

  @Override
  public void close() {
    metrics.removeMetric(numRestoringMetricName);
    metrics.removeSensor(NUM_INTERRUPTED_CHANGELOGS);
    metrics.removeSensor(RESTORE_LATENCY);
  }

}