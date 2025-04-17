package dev.responsive.kafka.internal.snapshot;

import org.apache.kafka.streams.processor.TaskId;

public class SnapshotStoreBasedGenerationStorage implements GenerationStorage {
  private final SnapshotStore snapshotStore;

  public SnapshotStoreBasedGenerationStorage(final SnapshotStore snapshotStore) {
    this.snapshotStore = snapshotStore;
  }

  @Override
  public long lookupGeneration(final TaskId taskId) {
    final var currentSnapshot = snapshotStore.currentSnapshot(false);
    // todo: move to a fn
    if (currentSnapshot.state() == Snapshot.State.COMPLETED
        || currentSnapshot.state() == Snapshot.State.FAILED ) {
      return currentSnapshot.generation();
    }
    if (currentSnapshot.taskSnapshots().stream()
        .anyMatch(s -> s.taskId().equals(taskId))) {
      return currentSnapshot.generation();
    }
    // this task has not completed. set generation to previous generation
    // todo: make previous generation a field rather than computing it here
    return currentSnapshot.generation() - 1;
  }
}
