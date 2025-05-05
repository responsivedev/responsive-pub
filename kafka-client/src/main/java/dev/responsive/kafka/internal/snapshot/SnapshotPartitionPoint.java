package dev.responsive.kafka.internal.snapshot;

import dev.responsive.kafka.internal.utils.PartitionPoint;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SnapshotPartitionPoint {
  public static <K, V> void printPartitionPoint(
      final SnapshotApi api,
      final Function<Snapshot, ReadOnlyKeyValueLogicalStore<K, V>> storeFactory,
      final Function<ReadOnlyKeyValueLogicalStore<K, V>, Boolean> predicate
  ) {
    final Optional<Long> partitionPoint = partitionPoint(api, storeFactory, predicate);
    if (partitionPoint.isEmpty()) {
      System.out.println("No snapshot found matching condition");
      return;
    }
    final List<Snapshot> snapshots = api.getSnapshots();
    Snapshot previous = null;
    Snapshot snapshot = null;
    for (final Snapshot s : snapshots) {
      if (s.generation() < partitionPoint.get()
          && (previous == null || s.generation() > previous.generation())) {
        previous = s;
      }
      if (s.generation() == partitionPoint.get()) {
        snapshot = s;
      }
    }
    assert snapshot != null;
    System.out.printf("Found snapshot matching condition with generation %d at %s\n",
        snapshot.generation(),
        snapshot.createdAt()
    );
    if (previous != null) {
      System.out.printf("Previous snapshot not matching condition with generation %d at %s\n",
          previous.generation(),
          previous.createdAt()
      );
    }
  }

  public static <K, V> Optional<Long> partitionPoint(
      final SnapshotApi api,
      final Function<Snapshot, ReadOnlyKeyValueLogicalStore<K, V>> storeFactory,
      final Function<ReadOnlyKeyValueLogicalStore<K, V>, Boolean> predicate
  ) {
    return partitionPoint(api, Instant.MIN, Instant.MAX, storeFactory, predicate);
  }

  /**
   * Compute the generation of the snapshot that represents the partition point
   * in the Snapshots returned by SnapshotApi. The snapshots are assumed to be
   * partitioned by the given predicate. This means that all elements for which
   * the predicate returns false are at the start of the list of snapshots, and
   * all elements for which the predicate returns true are at the end.
   *
   * @param api SnapshotApi used to get the list of snapshots
   * @param start The timestamp at which to begin the search (inclusive)
   * @param end The timestamp at which to end the search (inclusive)
   * @param storeFactory A factory function that constructs a read-only store from a snapshot
   * @param predicate The predicate to classify a given snapshot
   *
   * @return The generation of the first snapshot for which the predicate returns true
   *         If no snapshots return true, then return empty
   */
  public static <K, V> Optional<Long> partitionPoint(
      final SnapshotApi api,
      final Instant start,
      final Instant end,
      final Function<Snapshot, ReadOnlyKeyValueLogicalStore<K, V>> storeFactory,
      final Function<ReadOnlyKeyValueLogicalStore<K, V>, Boolean> predicate
  ) {
    final List<Snapshot> snapshots = api.getSnapshots().stream()
        .filter(s -> s.createdAt().isAfter(start) || s.createdAt().equals(start))
        .filter(s -> s.createdAt().isBefore(end) || s.createdAt().equals(end))
        .collect(Collectors.toList());
    if (snapshots.isEmpty()) {
      return Optional.empty();
    }
    return partitionPoint(snapshots, storeFactory, predicate);
  }

  public static <K, V> Optional<Long> partitionPoint(
      final List<Snapshot> snapshots,
      final Function<Snapshot, ReadOnlyKeyValueLogicalStore<K, V>> storeFactory,
      final Function<ReadOnlyKeyValueLogicalStore<K, V>, Boolean> predicate
  ) {
    final Optional<Integer> partitionPoint = PartitionPoint.partitionPoint(
        snapshots,
        s -> {
          final ReadOnlyKeyValueLogicalStore<K, V> store = storeFactory.apply(s);
          return predicate.apply(store);
        }
    );
    return partitionPoint.map(p -> snapshots.get(p).generation());
  }

  private SnapshotPartitionPoint() {
  }
}
