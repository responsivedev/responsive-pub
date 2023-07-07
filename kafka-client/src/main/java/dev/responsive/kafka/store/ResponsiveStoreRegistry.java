package dev.responsive.kafka.store;

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
    return getRegisteredStoresForChangelog(topicPartition).stream()
        .mapToLong(ResponsiveStoreRegistration::getCommittedOffset)
        .max();
  }

  public synchronized List<ResponsiveStoreRegistration> getRegisteredStoresForChangelog(
      final TopicPartition topicPartition
  ) {
    return stores.stream()
        .filter(s -> s.getChangelogTopicPartition().equals(topicPartition))
        .collect(Collectors.toList());
  }

  public synchronized void registerStore(final ResponsiveStoreRegistration registration) {
    validateSingleMaterialization(registration);
    stores.add(registration);
  }

  public synchronized void deregisterStore(final ResponsiveStoreRegistration registration) {
    stores.remove(registration);
  }

  private void validateSingleMaterialization(final ResponsiveStoreRegistration registration) {
    final String topic = registration.getChangelogTopicPartition().topic();
    final String name = registration.getName();
    final Optional<ResponsiveStoreRegistration> conflicting = stores.stream()
        .filter(si -> si.getChangelogTopicPartition().topic().equals(topic)
            && !si.getName().equals(name))
        .findFirst();
    if (conflicting.isPresent()) {
      final var err = new IllegalStateException(String.format(
          "Found two stores that materialize the same changelog topic (%s): %s, %s",
          topic,
          name, conflicting.get().getName()
      ));
      LOGGER.error("found conflicting materialization", err);
      throw err;
    }
  }
}
