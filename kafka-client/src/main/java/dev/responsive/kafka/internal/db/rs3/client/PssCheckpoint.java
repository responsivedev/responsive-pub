package dev.responsive.kafka.internal.db.rs3.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;
import java.util.UUID;

public class PssCheckpoint {
  private final UUID storeId;
  private final int pssId;
  private final StorageCheckpoint checkpoint;

  @JsonCreator
  public PssCheckpoint(
      @JsonProperty("storeId") final UUID storeId,
      @JsonProperty("pssId") final int pssId,
      @JsonProperty("checkpoint") StorageCheckpoint checkpoint,
      @JsonProperty("slateDbStorageCheckpoint")
      final SlateDbStorageCheckpoint slateDbStorageCheckpoint
  ) {
    this.storeId = Objects.requireNonNull(storeId);
    this.pssId = pssId;
    if (slateDbStorageCheckpoint != null) {
      this.checkpoint = slateDbStorageCheckpoint;
    } else {
      this.checkpoint = Objects.requireNonNull(checkpoint);
    }
  }

  @JsonProperty("storeId")
  public UUID getStoreId() {
    return storeId;
  }

  @JsonProperty("pssId")
  public int pssId() {
    return pssId;
  }

  @JsonProperty("checkpoint")
  public StorageCheckpoint checkpoint() {
    return checkpoint;
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
        && Objects.equals(checkpoint, that.checkpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeId, pssId, checkpoint);
  }

  public static final String SLATEDB_STORAGE_TYPE = "SlateDbStorage";

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.EXISTING_PROPERTY,
      property = "type",
      visible = true,
      defaultImpl = SlateDbStorageCheckpoint.class
  )
  @JsonSubTypes({
      @JsonSubTypes.Type(value = SlateDbStorageCheckpoint.class, name = SLATEDB_STORAGE_TYPE)
  })
  public abstract static class StorageCheckpoint {
    private final String type;

    StorageCheckpoint(final String type) {
      this.type = type;
    }

    @JsonProperty("type")
    public String type() {
      return type;
    }

    public abstract void visit(CheckpointVisitor visitor);

    public abstract <T> T map(CheckpointMapper<T> mapper);
  }

  public interface CheckpointVisitor {
    void visit(SlateDbStorageCheckpoint checkpoint);
  }

  public interface CheckpointMapper<T> {
    T map(SlateDbStorageCheckpoint checkpoint);
  }

  public static class SlateDbStorageCheckpoint extends StorageCheckpoint {
    private final String path;
    private final UUID checkpointId;

    @JsonCreator
    public SlateDbStorageCheckpoint(
        @JsonProperty("type") final String type,
        @JsonProperty("path") final String path,
        @JsonProperty("checkpointId") final UUID checkpointId
    ) {
      super(type);
      this.path = path;
      this.checkpointId = checkpointId;
    }

    public SlateDbStorageCheckpoint(
        @JsonProperty("path") final String path,
        @JsonProperty("checkpointId") final UUID checkpointId
    ) {
      super(SLATEDB_STORAGE_TYPE);
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
    public void visit(final CheckpointVisitor visitor) {
      visitor.visit(this);
    }

    @Override
    public <T> T map(final CheckpointMapper<T> mapper) {
      return mapper.map(this);
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
