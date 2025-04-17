package dev.responsive.kafka.internal.snapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TestSnapshotStore implements SnapshotStore {
  private final Map<Long, Snapshot> snapshots = new HashMap<>();

  public TestSnapshotStore() {
    final var initial = Snapshot.initial();
    snapshots.put(initial.generation(), initial);
  }

  @Override
  public synchronized Snapshot currentSnapshot(final boolean block) {
    return snapshots.get(snapshots.keySet().stream().max(Long::compare).get());
  }

  @Override
  public synchronized List<Snapshot> listSnapshots(final boolean block) {
    return new ArrayList<>(snapshots.values());
  }

  @Override
  public synchronized Snapshot updateCurrentSnapshot(final Function<Snapshot, Snapshot> updater) {
    final var updated = updater.apply(currentSnapshot(false));
    snapshots.put(updated.generation(), updated);
    return updated;
  }

  @Override
  public void close() {
  }
}
