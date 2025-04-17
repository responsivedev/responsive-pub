package dev.responsive.kafka.internal.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.processor.TaskId;

public class Snapshot {
  public enum State {
    CREATED,
    COMPLETED,
    // TODO: add state between CREATED and COMPLETED to reflect a snapshot
    //       that needs to be finalized by converting expiring checkpoints to
    //       nonexpiring checkpoints
    FAILED
    // TODO: add state beteen CREATED and FAILED to reflect a snapshot that has
    //       failed but is not yet cleaned up
  }

  private final Instant createdAt;
  private final long generation;
  private final State state;
  private final List<TaskSnapshotMetadata> taskSnapshots;

  public static Snapshot initial() {
    return new Snapshot(
        Instant.EPOCH,
        0,
        State.COMPLETED,
        List.of()
    );
  }

  @JsonCreator
  public Snapshot(
      @JsonProperty("createdAt") final Instant createdAt,
      @JsonProperty("generation") final long generation,
      @JsonProperty("state") final State state,
      @JsonProperty("taskSnapshots") final List<TaskSnapshotMetadata> taskSnapshots
  ) {
    this.createdAt = createdAt;
    this.generation = generation;
    this.state = state;
    this.taskSnapshots = List.copyOf(taskSnapshots);
  }

  @JsonProperty("createdAt")
  public Instant createdAt() {
    return createdAt;
  }

  @JsonProperty("generation")
  public long generation() {
    return generation;
  }

  @JsonProperty("state")
  public State state() {
    return state;
  }

  @JsonProperty("taskSnapshots")
  public List<TaskSnapshotMetadata> taskSnapshots() {
    return taskSnapshots;
  }

  public Snapshot nextSnapshot() {
    return new Snapshot(
        Instant.now(),
        generation + 1,
        State.CREATED,
        List.of()
    );
  }

  public Snapshot withTaskSnapshots(List<TaskSnapshotMetadata> taskSnapshots, State state) {
    return new Snapshot(
        createdAt,
        generation,
        state,
        Stream.concat(this.taskSnapshots.stream(), taskSnapshots.stream())
            .collect(Collectors.toList())
    );
  }

  public Snapshot withStateFailed() {
    return new Snapshot(
        createdAt,
        generation,
        State.FAILED,
        taskSnapshots
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Snapshot)) {
      return false;
    }
    final Snapshot snapshot = (Snapshot) o;
    return generation == snapshot.generation
        && Objects.equals(createdAt, snapshot.createdAt)
        && state == snapshot.state && Objects.equals(taskSnapshots, snapshot.taskSnapshots);
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdAt, generation, state, taskSnapshots);
  }

  @Override
  public String toString() {
    return "Snapshot{"
        + "createdAt=" + createdAt
        + ", generation=" + generation
        + ", state=" + state
        + ", taskSnapshots=" + taskSnapshots
        + '}';
  }

  public static class TaskSnapshotMetadata {
    private final TaskId taskId;
    private final List<CommittedOffset> committedOffsets;
    private final Map<String, byte[]> checkpoints;
    private final Instant timestamp;

    @JsonCreator
    public TaskSnapshotMetadata(
        @JsonProperty("taskId") final TaskId taskId,
        @JsonProperty("committedOffsets") final List<CommittedOffset> committedOffsets,
        @JsonProperty("checkpoints") final Map<String, byte[]> checkpoints,
        @JsonProperty("timestamp") final Instant timestamp
    ) {
      this.taskId = taskId;
      this.committedOffsets = List.copyOf(committedOffsets);
      this.checkpoints = Map.copyOf(checkpoints);
      this.timestamp = Objects.requireNonNull(timestamp);
    }

    @JsonProperty("taskId")
    public TaskId taskId() {
      return taskId;
    }

    @JsonProperty("committedOffsets")
    public List<CommittedOffset> committedOffsets() {
      return committedOffsets;
    }

    @JsonProperty("checkpoints")
    public Map<String, byte[]> checkpoints() {
      return checkpoints;
    }

    @JsonProperty("timestamp")
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TaskSnapshotMetadata)) {
        return false;
      }
      final TaskSnapshotMetadata that = (TaskSnapshotMetadata) o;
      if (checkpoints == null ^ that.checkpoints == null) {
        return false;
      }
      if (checkpoints != null) {
        if (checkpoints.size() != that.checkpoints.size()) {
          return false;
        }
        for (final Map.Entry<String, byte[]> entry : checkpoints.entrySet()) {
          if (!Arrays.equals(entry.getValue(), that.checkpoints.get(entry.getKey()))) {
            return false;
          }
        }
      }
      return Objects.equals(taskId, that.taskId)
          && Objects.equals(committedOffsets, that.committedOffsets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskId, committedOffsets, checkpoints);
    }

    @Override
    public String toString() {
      return "TaskSnapshotMetadata{"
          + "taskId=" + taskId
          + ", committedOffsets=" + committedOffsets
          + ", checkpoints=" + checkpoints
          + '}';
    }
  }

  public static class CommittedOffset {
    private final String topic;
    private final int partition;
    private final long offset;

    @JsonCreator
    public CommittedOffset(
        @JsonProperty("topic") final String topic,
        @JsonProperty("partition") final int partition,
        @JsonProperty("offset") final long offset
    ) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    @JsonProperty("topic")
    public String topic() {
      return topic;
    }

    @JsonProperty("partition")
    public int partition() {
      return partition;
    }

    @JsonProperty("offset")
    public long offset() {
      return offset;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CommittedOffset)) {
        return false;
      }
      final CommittedOffset that = (CommittedOffset) o;
      return partition == that.partition
          && offset == that.offset
          && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
      return "CommittedOffset{"
          + "topic='" + topic + '\''
          + ", partition=" + partition
          + ", offset=" + offset
          + '}';
    }
  }
}