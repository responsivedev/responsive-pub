package dev.responsive.kafka.clients;

import dev.responsive.kafka.store.ResponsiveRestoreListener;
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
  private final ResponsiveRestoreListener restoreListener;
  private final Set<TopicPartition> pausedChangelogPartitions = new HashSet<>();

  public ResponsiveRestoreConsumer(
      final String clientId,
      final Consumer<K, V> delegate,
      final Function<TopicPartition, OptionalLong> startOffsets,
      final ResponsiveRestoreListener restoreListener
  ) {
    super(delegate);

    this.startOffsets = Objects.requireNonNull(startOffsets);
    this.restoreListener = Objects.requireNonNull(restoreListener);
    this.log = new LogContext(
        String.format("responsive-restore-consumer [%s]", Objects.requireNonNull(clientId))
    ).logger(ResponsiveConsumer.class);

    restoreListener.addRestoreConsumer(this);
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

    pausedChangelogPartitions.retainAll(partitions);

    super.assign(partitions);
  }

  @Override
  public void unsubscribe() {
    uninitializedOffsets.clear();
    pausedChangelogPartitions.clear();
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

  @Override
  public void pause(final Collection<TopicPartition> partitions) {
    final Set<TopicPartition> toPause = new HashSet<>(pausedChangelogPartitions);
    toPause.addAll(partitions);
    super.pause(toPause);
  }

  @Override
  public void resume(final Collection<TopicPartition> partitions) {
    final Set<TopicPartition> toResume = new HashSet<>(partitions);
    toResume.removeAll(pausedChangelogPartitions);
    super.resume(toResume);
  }

  @Override
  public void close() {
    restoreListener.removeRestoreConsumer(this);
    super.close();
  }

  @Override
  public void close(final Duration timeout) {
    restoreListener.removeRestoreConsumer(this);
    super.close(timeout);
  }

  /**
   * Mark this changelog partition as a (potential) standby to block restoration.
   * <p>
   * This method is invoked on all changelogs during changelog registration, which begins with a
   * {@link #seek}. If the changelog is actually from an active task, it will be removed from this
   * set and resumed immediately afterward when the changelog registration completes.
   * See {@link #markChangelogAsActive(TopicPartition)} for more details
   */
  public void markChangelogAsStandby(final TopicPartition partition) {
    pausedChangelogPartitions.add(partition);
  }

  /**
   * Mark this changelog partition as active to allow restoration to proceed.
   * <p>
   * This method is invoked on active task changelogs only, at the end of the changelog registration
   * process. It is called from {@link ResponsiveRestoreListener#onRestoreStart} which is itself
   * invoked only for active tasks, allowing us to differentiate between active and standby
   * restoration.
   * See {@link #markChangelogAsStandby(TopicPartition)} for more details
   */
  public void markChangelogAsActive(final TopicPartition partition) {
    pausedChangelogPartitions.remove(partition);
  }

}
