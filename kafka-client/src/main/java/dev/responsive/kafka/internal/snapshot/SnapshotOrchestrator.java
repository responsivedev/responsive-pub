package dev.responsive.kafka.internal.snapshot;

import java.util.List;

public interface SnapshotOrchestrator {
  long getCurrentGeneration();

  void reportTaskSnapshotMetadata(
      long snapshotGeneration,
      List<Snapshot.TaskSnapshotMetadata> metadata
  );

  void failSnapshot(long snapshotGeneration);

  void close();
}
