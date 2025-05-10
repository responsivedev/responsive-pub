package dev.responsive.kafka.internal.snapshot;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of SnapshotApi that directly interacts with the Snapshot Store rather
 * than calling out to an API service.
 */
public class LocalSnapshotApi implements SnapshotApi {
  private final SnapshotStore snapshotStore;

  public LocalSnapshotApi(final SnapshotStore snapshotStore) {
    this.snapshotStore = Objects.requireNonNull(snapshotStore);
  }

  @Override
  public Snapshot createSnapshot() {
    return snapshotStore.updateCurrentSnapshot(snapshot -> {
      if (snapshot.state().equals(Snapshot.State.CREATED)) {
        throw new RuntimeException("Snapshot is currently in progress");
      }
      return snapshot.nextSnapshot();
    });
  }

  @Override
  public Snapshot getCurrentSnapshot() {
    return snapshotStore.currentSnapshot(true);
  }

  @Override
  public List<Snapshot> getSnapshots() {
    return snapshotStore.listSnapshots(true);
  }

  @Override
  public void close() {
    snapshotStore.close();
  }
}
