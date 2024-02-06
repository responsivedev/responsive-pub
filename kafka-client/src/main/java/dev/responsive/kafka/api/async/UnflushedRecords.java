package dev.responsive.kafka.api.async;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

public final class UnflushedRecords {
  private final List<UnflushedRecord> records;

  @JsonCreator
  public UnflushedRecords(@JsonProperty("records") final List<UnflushedRecord> records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @JsonProperty("records")
  public List<UnflushedRecord> records() {
    return records;
  }

  public static class UnflushedRecord {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    @JsonCreator
    public UnflushedRecord(
        final @JsonProperty("offset") long offset,
        final @JsonProperty("timestamp") long timestamp,
        final @JsonProperty("key") byte[] key,
        final @JsonProperty("value") byte[] value
    ) {
      this.offset = offset;
      this.timestamp = timestamp;
      this.key = key;
      this.value = value;
    }

    @JsonProperty("offset")
    public long offset() {
      return offset;
    }

    @JsonProperty("timestamp")
    public long timestamp() {
      return timestamp;
    }

    @JsonProperty("key")
    public byte[] key() {
      return key;
    }

    @JsonProperty("value")
    public byte[] value() {
      return value;
    }
  }
}
