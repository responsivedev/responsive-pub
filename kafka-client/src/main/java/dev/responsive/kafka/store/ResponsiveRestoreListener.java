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

package dev.responsive.kafka.store;

import dev.responsive.kafka.clients.ResponsiveRestoreConsumer;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A listener that can be registered by users to monitor the progress of state store restoration.
 * The listener callbacks are invoked on individual state store changelogs as they progress
 * through active restoration -- none of them will ever be called for standby tasks.
 * <p>
 * Note: this is a global object which is shared across all StreamThreads and state stores in
 * this Streams application.
 */
public class ResponsiveRestoreListener implements StateRestoreListener {
  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveRestoreListener.class);

  private final Metrics metrics;
  private final Set<ResponsiveRestoreConsumer<?, ?>> restoreConsumers = new LinkedHashSet<>();

  private StateRestoreListener userListener;

  public ResponsiveRestoreListener(final Metrics metrics) {
    this.metrics = metrics;
  }

  public void registerUserRestoreListener(final StateRestoreListener restoreListener) {
    userListener = restoreListener;
  }

  public StateRestoreListener userListener() {
    return userListener;
  }

  public synchronized void addRestoreConsumer(final ResponsiveRestoreConsumer<?, ?> restoreConsumer) {
    restoreConsumers.add(restoreConsumer);
  }

  public synchronized void removeRestoreConsumer(final ResponsiveRestoreConsumer<?, ?> restoreConsumer) {
    restoreConsumers.remove(restoreConsumer);
  }

  // TODO: once we split up the global ResponsiveStoreRegistry by StreamThread, we can use that
  //  to direct the call to the correct restore consumer
  @Override
  public synchronized void onRestoreStart(
      final TopicPartition topicPartition,
      final String storeName,
      final long startingOffset,
      final long endingOffset
  ) {
    LOG.info("Beginning restoration from offset {} to {} for partition {} of state store {}",
             startingOffset, endingOffset, topicPartition, storeName);

    // Leverage the listener to indicate which changelogs are active, as these callbacks
    // are never invoked for the state stores of standby tasks
    restoreConsumers.forEach(rc -> rc.markChangelogAsActive(topicPartition));

    if (userListener != null) {
      userListener.onRestoreStart(
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

    if (userListener != null) {
      userListener.onBatchRestored(
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

    if (userListener != null) {
      userListener.onRestoreEnd(
          topicPartition,
          storeName,
          totalRestored);
    }
  }

}
