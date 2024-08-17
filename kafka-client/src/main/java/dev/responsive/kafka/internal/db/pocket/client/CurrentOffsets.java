package dev.responsive.kafka.internal.db.pocket.client;

import java.util.Optional;

public class CurrentOffsets {
  private final Optional<Long> writtenOffset;
  private final Optional<Long> flushedOffset;

  public CurrentOffsets(final Optional<Long> writtenOffset, final Optional<Long> flushedOffset) {
    this.writtenOffset = writtenOffset;
    this.flushedOffset = flushedOffset;
  }

  public Optional<Long> writtenOffset() {
    return writtenOffset;
  }

  public Optional<Long> flushedOffset() {
    return flushedOffset;
  }
}
