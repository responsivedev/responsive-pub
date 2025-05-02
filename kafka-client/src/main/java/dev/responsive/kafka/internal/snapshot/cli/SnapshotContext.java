package dev.responsive.kafka.internal.snapshot.cli;

import dev.responsive.kafka.internal.snapshot.SnapshotApi;
import java.util.Objects;

class SnapshotContext {
  private final SnapshotApi api;

  SnapshotContext(final SnapshotApi api) {
    this.api = Objects.requireNonNull(api, "snapshotApi");
  }

  SnapshotApi api() {
    return api;
  }

  void close() {
    api.close();
  }
}
