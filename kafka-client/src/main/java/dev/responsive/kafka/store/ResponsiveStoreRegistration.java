package dev.responsive.kafka.store;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;

public final class ResponsiveStoreRegistration {
  private final TopicPartition changelogTopicPartition;
  private final long committedOffset;
  private final String name;
  private final Consumer<Long> onCommit;

  @VisibleForTesting
  public ResponsiveStoreRegistration(
      final String name,
      final TopicPartition changelogTopicPartition,
      final long committedOffset,
      final Consumer<Long> onCommit
  ) {
    this.name = Objects.requireNonNull(name);
    this.changelogTopicPartition = Objects.requireNonNull(changelogTopicPartition);
    this.committedOffset = committedOffset;
    this.onCommit = Objects.requireNonNull(onCommit);
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

  public Consumer<Long> getOnCommit() {
    return onCommit;
  }
}
