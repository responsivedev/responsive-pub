package dev.responsive.kafka.clients;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Consumer implementation to use for restoring Responsive stores. This Restore Consumer emulates
 * consumping a topic that has been truncated up to some offset on a subset of topic partitions.
 * The specific offsets are provided by a function mapping the topic partition to the start
 * offset. This consumer is used in conjunction with Responsive Stores, which provide a safe
 * starting offset that has been committed to a remote storage system. This is useful for
 * minimizing the length of restores when using a Responsive Store over a compacted changelog
 * topic (which cannot be truncated).
 *
 * @param <K> Type of the record keys
 * @param <V> Type of the record values
 */
public class ResponsiveRestoreConsumer<K, V> extends DelegatingConsumer<K, V> {
  final Function<TopicPartition, OptionalLong> startOffsets;

  public ResponsiveRestoreConsumer(
      final Consumer<K, V> delegate,
      final Function<TopicPartition, OptionalLong> startOffsets
  ) {
    super(delegate);
    this.startOffsets = Objects.requireNonNull(startOffsets);
  }

  private List<TopicPartition> seekInitial(final Collection<TopicPartition> partitions) {
    final List<TopicPartition> notFound = new LinkedList<>();
    for (final TopicPartition p : partitions) {
      final OptionalLong start = startOffsets.apply(p);
      if (start.isPresent()) {
        delegate.seek(p, start.getAsLong());
      } else {
        notFound.add(p);
      }
    }
    return Collections.unmodifiableList(notFound);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    super.assign(partitions);
    seekInitial(partitions);
  }

  @Override
  public void seek(final TopicPartition partition, final long offset) {
    super.seek(partition, Math.max(offset, startOffsets.apply(partition).orElse(offset)));
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
  }

  @Override
  public void seekToBeginning(final Collection<TopicPartition> partitions) {
    final Collection<TopicPartition> withUncachedPosition = seekInitial(partitions);
    if (withUncachedPosition.size() > 0) {
      // Make sure to only do this if the list of partitions without cached start positions
      // is not empty. If it's empty, KafkaConsumer will seek to the actual beginning of _all_
      // partitions.
      super.seekToBeginning(withUncachedPosition);
    }
  }
}
