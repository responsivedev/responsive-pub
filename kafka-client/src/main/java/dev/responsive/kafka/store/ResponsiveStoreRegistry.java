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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: there is exactly one instance per Streams application, shared between all threads. This
 * means we may track multiple versions of the same state store, for example an active/standby and
 * one or more zombies.
 */
// TODO could solve the above issue by creating one store registry per StreamThread, which
//  would of course also let us remove all the synchronization as well. Which is nice
public class ResponsiveStoreRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveStoreRegistry.class);

  private final Map<TopicPartition, List<ResponsiveStoreRegistration>> stores = new HashMap<>();

  public synchronized OptionalLong startOffset(final TopicPartition topicPartition) {
    return getRegisteredStoresForChangelog(topicPartition).stream()
        .mapToLong(ResponsiveStoreRegistration::startOffset)
        .max();
  }

  // TODO: we return potentially more than one store because there might be zombie stores that
  //  haven't yet been closed/unregistered -- but we can just bump the zombies to maintain
  //  and allow only a single valid registered store at a time (eg via onRestoreStart)
  public synchronized List<ResponsiveStoreRegistration> getRegisteredStoresForChangelog(
      final TopicPartition topicPartition
  ) {
    return stores.get(topicPartition);
  }

  public synchronized void registerStore(final ResponsiveStoreRegistration registration) {
    validateSingleMaterialization(registration);
    stores
        .computeIfAbsent(registration.changelogTopicPartition(), x -> new LinkedList<>())
        .add(registration);
  }

  public synchronized void deregisterStore(final ResponsiveStoreRegistration registration) {
    stores.remove(registration.changelogTopicPartition());
  }

  public synchronized List<ResponsiveStoreRegistration> stores() {
    return stores.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  // Validate that no two stores of different names attempt to materialize the same changelog topic
  private void validateSingleMaterialization(final ResponsiveStoreRegistration registration) {
    final String topic = registration.changelogTopicPartition().topic();
    final String name = registration.storeName();
    final Optional<ResponsiveStoreRegistration> conflicting = stores.entrySet().stream()
        .flatMap(e -> e.getValue().stream())
        .filter(si -> si.changelogTopicPartition().topic().equals(topic)
            && !si.storeName().equals(name))
        .findFirst();
    if (conflicting.isPresent()) {
      final var err = new IllegalStateException(String.format(
          "Found two stores that materialize the same changelog topic (%s): %s, %s",
          topic,
          name, conflicting.get().storeName()
      ));
      LOG.error("Found conflicting materialization", err);
      throw err;
    }
  }
}
