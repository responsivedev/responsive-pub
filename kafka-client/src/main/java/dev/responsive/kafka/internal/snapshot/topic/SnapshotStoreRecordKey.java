package dev.responsive.kafka.internal.snapshot.topic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;

public class SnapshotStoreRecordKey {

  private final SnapshotStoreRecordType type;
  private final Long generation;

  @JsonCreator
  public SnapshotStoreRecordKey(
      @JsonProperty("type") final SnapshotStoreRecordType type,
      @JsonProperty("generation") final Long generation
  ) {
    this.type = Objects.requireNonNull(type);
    this.generation = generation;
  }

  @JsonProperty("type")
  public SnapshotStoreRecordType type() {
    return type;
  }

  @JsonProperty("generation")
  public Optional<Long> generation() {
    return Optional.ofNullable(generation);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SnapshotStoreRecordKey)) {
      return false;
    }
    final SnapshotStoreRecordKey that = (SnapshotStoreRecordKey) o;
    return type == that.type && Objects.equals(generation, that.generation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, generation);
  }

  @Override
  public String toString() {
    return "SnapshotStoreRecordKey{"
        + "type=" + type
        + ", generation=" + generation
        + '}';
  }
}