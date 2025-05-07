/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Objects;
import java.util.UUID;

public class CreateCheckpointResult {

  private final StorageCheckpoint checkpoint;

  public CreateCheckpointResult(StorageCheckpoint checkpoint) {
    this.checkpoint = checkpoint;
  }

  public StorageCheckpoint checkpoint() {
    return checkpoint;
  }

  @Override
  public String toString() {
    return "CreateCheckpointResult{"
        + "checkpoint=" + checkpoint
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateCheckpointResult that = (CreateCheckpointResult) o;
    return Objects.equals(checkpoint, that.checkpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(checkpoint);
  }

  public interface StorageCheckpoint {

    <T> T map(CheckpointMapper<T> mapper);

    void visit(CheckpointVisitor visitor);
  }

  public interface CheckpointVisitor {
    void visit(SlatedbCheckpoint checkpoint);
  }

  public interface CheckpointMapper<T> {
    T map(SlatedbCheckpoint checkpoint);
  }

  public static class SlatedbCheckpoint implements StorageCheckpoint {
    private final UUID id;
    private final String path;

    public SlatedbCheckpoint(final UUID id, final String path) {
      this.id = id;
      this.path = path;
    }

    public UUID id() {
      return id;
    }

    public String path() {
      return path;
    }

    @Override
    public String toString() {
      return "SlatedbCheckpoint{"
          + "id=" + id
          + ", path='" + path + '\''
          + '}';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SlatedbCheckpoint that = (SlatedbCheckpoint) o;
      return Objects.equals(id, that.id) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, path);
    }

    @Override
    public <T> T map(final CheckpointMapper<T> mapper) {
      return mapper.map(this);
    }

    @Override
    public void visit(final CheckpointVisitor visitor) {
      visitor.visit(this);
    }
  }
}
