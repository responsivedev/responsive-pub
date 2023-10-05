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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: this is a global object which is shared across all StreamThreads and state stores in
 * this Streams application.
 */
public class ResponsiveRestoreListener implements StateRestoreListener {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveRestoreListener.class);

  private final ResponsiveMetrics metrics;

  private StateRestoreListener userRestoreListener;

  public ResponsiveRestoreListener(final ResponsiveMetrics metrics) {
    this.metrics = metrics;
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

    if (userRestoreListener != null) {
      userRestoreListener.onRestoreEnd(
          topicPartition,
          storeName,
          totalRestored);
    }
  }

}