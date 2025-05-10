package dev.responsive.kafka.internal.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.processor.TaskId;

/**
 * An implementation of the orchestrator that runs within the application and interacts
 * directly with the snapshot store.
 */
public class LocalSnapshotOrchestrator implements SnapshotOrchestrator {
  private final SnapshotStore snapshotStore;
  private final Set<TaskId> allTasks;

  public LocalSnapshotOrchestrator(
      final SnapshotStore snapshotStore,
      final Set<TaskId> allTasks
  ) {
    this.snapshotStore = Objects.requireNonNull(snapshotStore);
    this.allTasks = Objects.requireNonNull(allTasks);
  }

  @Override
  public long getCurrentGeneration() {
    return snapshotStore.currentSnapshot(false).generation();
  }

  @Override
  public void reportTaskSnapshotMetadata(
      final long generation,
      final List<Snapshot.TaskSnapshotMetadata> taskSnapshots
  ) {
    snapshotStore.updateCurrentSnapshot(snapshot -> {
      // check that we're still working on this snapshot
      if (snapshot.generation() != generation) {
        throw new RuntimeException(
            String.format("generation too old: %d > %d", snapshot.generation(), generation));
      }
      if (!snapshot.state().equals(Snapshot.State.CREATED)) {
        throw new RuntimeException("Snapshot is currently completed. Cannot update");
      }

      // check that for all the specified tasks, we either haven't collected its metadata
      // or the metadata is the same
      final List<Snapshot.TaskSnapshotMetadata> newlyCompletedTaskSnapshots = new ArrayList<>();
      for (final var taskSnapshot : taskSnapshots) {
        final var found = snapshot.taskSnapshots()
            .stream()
            .filter(s -> s.taskId().equals(taskSnapshot.taskId()))
            .collect(Collectors.toList());
        if (found.size() > 1) {
          throw new IllegalStateException(
              "found multiple snapshots for task " + taskSnapshot.taskId());
        }
        if (found.isEmpty()) {
          newlyCompletedTaskSnapshots.add(taskSnapshot);
        } else if (!found.get(0).equals(taskSnapshot)) {
          throw new IllegalStateException(
              "found conflicting snapshots for task" + taskSnapshot.taskId());
        }
      }

      // if we've collected snapshots for all tasks, mark the snapshot as completed
      final Set<TaskId> completedTasks = Stream.concat(
          newlyCompletedTaskSnapshots.stream().map(Snapshot.TaskSnapshotMetadata::taskId),
          snapshot.taskSnapshots().stream().map(Snapshot.TaskSnapshotMetadata::taskId)
      ).collect(Collectors.toSet());
      Snapshot.State state = snapshot.state();
      if (completedTasks.equals(this.allTasks)) {
        state = Snapshot.State.COMPLETED;
      }

      return snapshot.withTaskSnapshots(newlyCompletedTaskSnapshots, state);
    });
  }

  @Override
  public void failSnapshot(long snapshotGeneration) {
    snapshotStore.updateCurrentSnapshot(snapshot -> {
      if (snapshot.generation() != snapshotGeneration) {
        // todo: do something more reasonable here
        throw new RuntimeException("Generation mismatch");
      }
      if (snapshot.state().equals(Snapshot.State.COMPLETED)) {
        throw new RuntimeException("Cannot fail completed snapshot");
      }
      return snapshot.withStateFailed();
    });
  }

  @Override
  public void close() {
    snapshotStore.close();
  }
}
