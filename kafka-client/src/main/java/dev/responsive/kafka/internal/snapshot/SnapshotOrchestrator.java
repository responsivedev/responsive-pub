package dev.responsive.kafka.internal.snapshot;

import java.util.List;

/**
 * SnapshotOrchestrator is responsible for coordinating snapshot execution by initiating
 * task-level snapshots and updating the snapshot's state as they complete.
 */
public interface SnapshotOrchestrator {
  /**
   * Gets the application's current snapshot generation
   *
   * @return The current snapshot generation.
   */
  long getCurrentGeneration();

  /**
   * Called by the stream thread to report task snapshot metadata for a given task.
   *
   * @param snapshotGeneration The generation of the task snapshot(s) being reported.
   * @param metadata The task snapshot(s) being reported.
   */
  void reportTaskSnapshotMetadata(
      long snapshotGeneration,
      List<Snapshot.TaskSnapshotMetadata> metadata
  );

  /**
   * Called by the stream thread to report a failed task snapshot. This should only
   * be called to report terminal failures.
   *
   * @param snapshotGeneration The generation of the snapshot to fail.
   */
  void failSnapshot(long snapshotGeneration);

  /**
   * Clean up any resources held by the orchestrator
   */
  void close();
}
