package dev.responsive.kafka.internal.clients;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * Consumer implementation to use for restoring Responsive stores. This Restore Consumer emulates
 * consuming a topic that has been truncated up to some offset on a subset of topic partitions.
 * The specific offsets are provided by a function mapping the topic partition to the start
 * offset. This consumer is used in conjunction with Responsive Stores, which provide a safe
 * starting offset that has been committed to a remote storage system. This is useful for
 * minimizing the length of restores when using a Responsive Store over a compacted changelog
 * topic (which cannot be truncated).
 * <p>
 * This consumer is used by Kafka Streams for both active and standby task restoration, though
 * only one or the other at a given time. This may be subject to change, but the general process
 * follows this pattern:
 * 1. initialize changelogs for newly assigned tasks:
 *     i. restoreConsumer.assign(current assignment + new changelogs)
 *     ii. if STANDBY_MODE: restoreConsumer.pause(standby changelogs)
 *     iii. restoreConsumer.seekToBeginning(new changelogs)
 *     iv. startOffset = restoreConsumer.position(changelog)
 *     iv. for each ACTIVE changelog: call StateRestoreListener#onRestoreStart
 * 2. restore active/standby state stores:
 *     i. poll for new record batch
 *     ii. restore batch through StateRestoreCallback
 *     iii. for each ACTIVE changelog in batch: call StateRestoreListener#onBatchRestored
 *     iv. update current offset based on highest offset in batch
 *
 * @param <K> Type of the record keys
 * @param <V> Type of the record values
 */
public class ResponsiveRestoreConsumer<K, V> extends DelegatingConsumer<K, V> {
  private final Logger log;

  private final Function<TopicPartition, OptionalLong> startOffsets;
  private final Set<TopicPartition> uninitializedOffsets = new HashSet<>();

  public ResponsiveRestoreConsumer(
      final String clientId,
      final Consumer<K, V> delegate,
      final Function<TopicPartition, OptionalLong> startOffsets
  ) {
    super(delegate);
    this.startOffsets = Objects.requireNonNull(startOffsets);
    this.log = new LogContext(
        String.format("responsive-restore-consumer [%s]", Objects.requireNonNull(clientId))
    ).logger(ResponsiveConsumer.class);
  }

  private Set<TopicPartition> initializeOffsets(final Collection<TopicPartition> partitions) {
    final Set<TopicPartition> notFound = new HashSet<>();
    for (final TopicPartition p : partitions) {
      final OptionalLong start = startOffsets.apply(p);
      if (start.isPresent()) {
        super.seek(p, start.getAsLong());
      } else {
        notFound.add(p);
      }
    }
    uninitializedOffsets.removeAll(partitions);
    return Collections.unmodifiableSet(notFound);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    // track any newly-assigned changelogs
    uninitializedOffsets.addAll(
        partitions.stream()
            .filter(p -> !super.assignment().contains(p))
            .collect(Collectors.toSet()));

    // clear any now-unassigned changelogs
    uninitializedOffsets.retainAll(partitions);

    super.assign(partitions);
  }

  @Override
  public void unsubscribe() {
    uninitializedOffsets.clear();
    super.unsubscribe();
  }

  @Override
  public ConsumerRecords<K, V> poll(final Duration timeout) {
    if (!uninitializedOffsets.isEmpty()) {
      log.error("Found uninitialized changelog partitions during poll: {}", uninitializedOffsets);
      throw new IllegalStateException("Restore consumer invoked poll without initializing offsets");
    }
    return super.poll(timeout);
  }

  @Override
  public void seek(final TopicPartition partition, final long offset) {
    super.seek(partition, Math.max(offset, startOffsets.apply(partition).orElse(offset)));
    uninitializedOffsets.remove(partition);
  }

  @Override
  public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
    super.seek(
        partition,
        new OffsetAndMetadata(
            Math.max(
                offsetAndMetadata.offset(),
                startOffsets.apply(partition).orElse(offsetAndMetadata.offset())
            ),
            offsetAndMetadata.leaderEpoch(),
            offsetAndMetadata.metadata()
        )
    );
    uninitializedOffsets.remove(partition);
  }

  @Override
  public void seekToBeginning(final Collection<TopicPartition> partitions) {
    final Set<TopicPartition> withUncachedPosition = initializeOffsets(partitions);
    if (withUncachedPosition.size() > 0) {
      // Make sure to only do this if the list of partitions without cached start positions
      // is not empty. If it's empty, KafkaConsumer will seek to the actual beginning of _all_
      // partitions.
      super.seekToBeginning(withUncachedPosition);
      uninitializedOffsets.removeAll(withUncachedPosition);
    }
  }
}
