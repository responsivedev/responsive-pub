package dev.responsive.kafka.internal.snapshot.topic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.responsive.kafka.internal.snapshot.Snapshot;
import java.util.Objects;
import java.util.Optional;

public class SnapshotStoreRecord {
  private final SnapshotStoreRecordType type;
  private final Snapshot snapshot;

  @JsonCreator
  public SnapshotStoreRecord(
      @JsonProperty("type") final SnapshotStoreRecordType type,
      @JsonProperty("snapshot") final Snapshot snapshot
  ) {
    this.type = Objects.requireNonNull(type);
    this.snapshot = snapshot;
  }

  @JsonProperty("type")
  public SnapshotStoreRecordType type() {
    return type;
  }

  @JsonProperty("snapshot")
  public Optional<Snapshot> snapshot() {
    return Optional.ofNullable(snapshot);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SnapshotStoreRecord)) {
      return false;
    }
    final SnapshotStoreRecord that = (SnapshotStoreRecord) o;
    return type == that.type && Objects.equals(snapshot, that.snapshot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, snapshot);
  }

  @Override
  public String toString() {
    return "SnapshotStoreRecord{"
        + "type=" + type
        + ", snapshot=" + snapshot
        + '}';
  }
}