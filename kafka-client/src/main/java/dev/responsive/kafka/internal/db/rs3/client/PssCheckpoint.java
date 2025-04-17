package dev.responsive.kafka.internal.db.rs3.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class PssCheckpoint {
  private final UUID storeId;
  private final int pssId;
  private final SlateDbStorageCheckpoint slateDbStorageCheckpoint;

  @JsonCreator
  public PssCheckpoint(
      @JsonProperty("storeId") final UUID storeId,
      @JsonProperty("pssId") final int pssId,
      @JsonProperty("slateDbStorageCheckpoint")
      final SlateDbStorageCheckpoint slateDbStorageCheckpoint
  ) {
    this.storeId = Objects.requireNonNull(storeId);
    this.pssId = pssId;
    this.slateDbStorageCheckpoint = slateDbStorageCheckpoint;
  }

  @JsonProperty("storeId")
  public UUID getStoreId() {
    return storeId;
  }

  @JsonProperty("pssId")
  public int pssId() {
    return pssId;
  }

  @JsonProperty("slateDbStorageCheckpoint")
  public Optional<SlateDbStorageCheckpoint> slateDbStorageCheckpoint() {
    return Optional.ofNullable(slateDbStorageCheckpoint);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PssCheckpoint)) {
      return false;
    }
    final PssCheckpoint that = (PssCheckpoint) o;
    return Objects.equals(storeId, that.storeId)
        && pssId == that.pssId
        && Objects.equals(slateDbStorageCheckpoint, that.slateDbStorageCheckpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeId, pssId, slateDbStorageCheckpoint);
  }

  public static class SlateDbStorageCheckpoint {
    private final String path;
    private final UUID checkpointId;

    @JsonCreator
    public SlateDbStorageCheckpoint(
        @JsonProperty("path") final String path,
        @JsonProperty("checkpointId") final UUID checkpointId
    ) {
      this.path = path;
      this.checkpointId = checkpointId;
    }

    @JsonProperty("path")
    public String path() {
      return path;
    }

    @JsonProperty("checkpointId")
    public UUID checkpointId() {
      return checkpointId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SlateDbStorageCheckpoint)) {
        return false;
      }
      final SlateDbStorageCheckpoint that = (SlateDbStorageCheckpoint) o;
      return Objects.equals(path, that.path)
          && Objects.deepEquals(checkpointId, that.checkpointId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, checkpointId);
    }

    @Override
    public String toString() {
      return "SlateDbStorageCheckpoint{"
          + "path='" + path + '\''
          + ", checkpointId=" + checkpointId
          + '}';
    }
  }
}
