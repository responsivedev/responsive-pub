package dev.responsive.kafka.internal.snapshot;

import java.util.List;

/**
 * Interface for interacting with Snapshots from outside an application. Supports
 * reading the current/past snapshots, and creating a new snapshot.
 */
public interface SnapshotApi extends AutoCloseable {
  Snapshot createSnapshot();

  List<Snapshot> getSnapshots();

  Snapshot getCurrentSnapshot();

  @Override
  void close();
}
