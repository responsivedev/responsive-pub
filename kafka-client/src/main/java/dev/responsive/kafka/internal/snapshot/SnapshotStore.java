package dev.responsive.kafka.internal.snapshot;

import java.util.List;
import java.util.function.Function;

/**
 * A store for the Snapshot metadata for a given applicaiton
 */
public interface SnapshotStore extends AutoCloseable {
  /**
   * Returns the current snapshot
   *
   * @param block if true, then the call blocks until the store has observed the latest update
   * @return the current snapshot of the application
   */
  Snapshot currentSnapshot(boolean block);

  /**
   * List all snapshots
   *
   * @param block if true, then the call will block until the store has observed the latest update
   * @return a list of all snapshots of the application
   */
  List<Snapshot> listSnapshots(boolean block);

  /**
   * Updates a snapshot given an updater function. The update is guaranteed to be isolated from
   * other concurrent updates.
   *
   * @param updater is passed the current snapshot and returns an updated snapshot.
   * @return the updated snapshot
   */
  Snapshot updateCurrentSnapshot(
      Function<Snapshot, Snapshot> updater
  );

  @Override
  void close();
}
