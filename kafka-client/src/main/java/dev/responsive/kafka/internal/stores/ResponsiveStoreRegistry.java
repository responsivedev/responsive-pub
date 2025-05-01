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

package dev.responsive.kafka.internal.stores;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveStoreRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResponsiveStoreRegistry.class);

  private final List<ResponsiveStoreRegistration> stores = new LinkedList<>();

  public synchronized OptionalLong getCommittedOffset(
      final TopicPartition topicPartition,
      final String threadId) {
    return getRegisteredStoresForChangelog(topicPartition, threadId)
        .stream()
        .map(ResponsiveStoreRegistration::startOffset)
        .filter(OptionalLong::isPresent)
        .mapToLong(OptionalLong::getAsLong)
        .max();
  }

  public synchronized List<ResponsiveStoreRegistration> getRegisteredStoresForChangelog(
      final TopicPartition topicPartition
  ) {
    return stores.stream()
        .filter(s -> s.changelogTopicPartition().equals(topicPartition))
        .collect(Collectors.toList());
  }

  private List<ResponsiveStoreRegistration> filterStoresForThread(
      final List<ResponsiveStoreRegistration> stores,
      final String threadId,
      final String context
  ) {
    if (stores.isEmpty()) {
      return stores;
    }
    final List<ResponsiveStoreRegistration> storesForThread = stores.stream()
        .filter(s -> s.threadId().equals(threadId))
        .collect(Collectors.toList());
    if (storesForThread.isEmpty()) {
      throw new IllegalStateException(String.format(
          "there should always be a store for the thread (%s) if there are stores registered "
              + "for this %s", threadId, context));
    }
    return stores;
  }

  public synchronized List<ResponsiveStoreRegistration> getRegisteredStoresForChangelog(
      final TopicPartition topicPartition,
      final String threadId
  ) {
    final List<ResponsiveStoreRegistration> storesForTopicPartition = stores.stream()
        .filter(s -> s.changelogTopicPartition().equals(topicPartition))
        .collect(Collectors.toList());
    return filterStoresForThread(
        storesForTopicPartition,
        threadId,
        String.format("topic partition (%s)", topicPartition)
    );
  }

  public synchronized List<ResponsiveStoreRegistration> getRegisteredStoresForTask(
      final TaskId taskId,
      final String threadId
  ) {
    final List<ResponsiveStoreRegistration> storesForTask = stores.stream()
        .filter(s -> s.taskId().equals(taskId))
        .collect(Collectors.toList());
    return filterStoresForThread(
        storesForTask,
        threadId,
        String.format("task (%s)", taskId)
    );
  }

  public synchronized void registerStore(final ResponsiveStoreRegistration registration) {
    validateSingleMaterialization(registration);
    stores.add(registration);
  }

  public synchronized void deregisterStore(final ResponsiveStoreRegistration registration) {
    stores.remove(registration);
  }

  public synchronized List<ResponsiveStoreRegistration> stores() {
    return stores;
  }

  private void validateSingleMaterialization(final ResponsiveStoreRegistration registration) {
    final String topic = registration.changelogTopicPartition().topic();
    final String name = registration.storeName();
    final Optional<ResponsiveStoreRegistration> conflicting = stores.stream()
        .filter(si -> si.changelogTopicPartition().topic().equals(topic)
            && !si.storeName().equals(name))
        .findFirst();
    if (conflicting.isPresent()) {
      final var err = new IllegalStateException(String.format(
          "Found two stores that materialize the same changelog topic (%s): %s, %s",
          topic,
          name, conflicting.get().storeName()
      ));
      LOGGER.error("found conflicting materialization", err);
      throw err;
    }
  }
}
