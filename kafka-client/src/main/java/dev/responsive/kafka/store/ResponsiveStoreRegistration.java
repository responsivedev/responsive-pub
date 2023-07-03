package dev.responsive.kafka.store;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

public final class ResponsiveStoreRegistration {
  private final TopicPartition changelogTopicPartition;
  private final long committedOffset;
  private final String name;

  ResponsiveStoreRegistration(
      final String name,
      final TopicPartition changelogTopicPartition,
      final long committedOffset
  ) {
    this.name = Objects.requireNonNull(name);
    this.changelogTopicPartition = Objects.requireNonNull(changelogTopicPartition);
    this.committedOffset = committedOffset;
  }

  public long getCommittedOffset() {
    return committedOffset;
  }

  public TopicPartition getChangelogTopicPartition() {
    return changelogTopicPartition;
  }

  public String getName() {
    return name;
  }
}
