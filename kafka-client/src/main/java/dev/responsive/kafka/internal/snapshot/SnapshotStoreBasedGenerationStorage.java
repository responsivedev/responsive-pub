package dev.responsive.kafka.internal.snapshot;

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotStoreBasedGenerationStorage implements GenerationStorage {
  private static final Logger LOG
      = LoggerFactory.getLogger(SnapshotStoreBasedGenerationStorage.class);

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
      LOG.info("found completed task {}. return current generation {}",
          taskId,
          currentSnapshot.generation()
      );
      return currentSnapshot.generation();
    }
    LOG.info("found uncompleted task {}. return previous generation {}",
        taskId,
        currentSnapshot.generation() - 1
    );
    // this task has not completed. set generation to previous generation
    // todo: make previous generation a field rather than computing it here
    return currentSnapshot.generation() - 1;
  }
}
