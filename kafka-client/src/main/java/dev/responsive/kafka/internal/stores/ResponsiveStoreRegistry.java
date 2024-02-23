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

import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveStoreRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResponsiveStoreRegistry.class);

  private final List<ResponsiveStoreRegistration> stores = new LinkedList<>();

  public synchronized OptionalLong getCommittedOffset(final TopicPartition topicPartition) {
    return getRegisteredStoresForChangelog(topicPartition)
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
