package dev.responsive.kafka.internal.db.rs3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableCheckpoint {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new Jdk8Module());
  }

  final List<TablePssCheckpoint> pssCheckpoints;

  @JsonCreator
  public TableCheckpoint(
      @JsonProperty("pssCheckpoints") final List<TablePssCheckpoint> pssCheckpoints
  ) {
    this.pssCheckpoints = List.copyOf(Objects.requireNonNull(pssCheckpoints));
  }

  @JsonProperty("pssCheckpoints")
  public List<TablePssCheckpoint> pssCheckpoints() {
    return pssCheckpoints;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableCheckpoint)) {
      return false;
    }
    final TableCheckpoint that = (TableCheckpoint) o;
    return Objects.equals(pssCheckpoints, that.pssCheckpoints);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pssCheckpoints);
  }

  public static class TablePssCheckpoint {
    final Optional<Long> writtenOffset;
    final PssCheckpoint checkpoint;

    @JsonCreator
    public TablePssCheckpoint(
        @JsonProperty("writtenOffset") final Optional<Long> writtenOffset,
        @JsonProperty("checkpoint") final PssCheckpoint checkpoint
    ) {
      this.writtenOffset = Objects.requireNonNull(writtenOffset);
      this.checkpoint = Objects.requireNonNull(checkpoint);
    }

    @JsonProperty("writtenOffset")
    public Optional<Long> writtenOffset() {
      return writtenOffset;
    }

    @JsonProperty("checkpoint")
    public PssCheckpoint checkpoint() {
      return checkpoint;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TablePssCheckpoint)) {
        return false;
      }
      final TablePssCheckpoint that = (TablePssCheckpoint) o;
      return Objects.equals(writtenOffset, that.writtenOffset)
          && Objects.equals(checkpoint, that.checkpoint);
    }

    @Override
    public int hashCode() {
      return Objects.hash(writtenOffset, checkpoint);
    }
  }

  @Override
  public String toString() {
    return new String(TableCheckpoint.serialize(this), Charset.defaultCharset());
  }

  public static byte[] serialize(TableCheckpoint tableCheckpoint) {
    try {
      return MAPPER.writeValueAsBytes(tableCheckpoint);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static TableCheckpoint deserialize(byte[] serialized) {
    try {
      return MAPPER.readValue(serialized, TableCheckpoint.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
