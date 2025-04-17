package dev.responsive.kafka.internal.snapshot;

import java.util.List;

public interface SnapshotApi extends AutoCloseable {
  Snapshot createSnapshot();

  List<Snapshot> getSnapshots();

  Snapshot getCurrentSnapshot();

  @Override
  void close();
}
