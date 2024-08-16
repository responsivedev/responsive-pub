package dev.responsive.kafka.internal.db.pocket.client;

import java.util.Optional;

public class CurrentOffsets {
  private final Long writtenOffset;
  private final Long flushedOffset;

  public CurrentOffsets(final Long writtenOffset, final Long flushedOffset) {
    this.writtenOffset = writtenOffset;
    this.flushedOffset = flushedOffset;
  }

  public Optional<Long> writtenOffset() {
    return Optional.ofNullable(writtenOffset);
  }

  public Optional<Long> flushedOffset() {
    return Optional.ofNullable(flushedOffset);
  }
}
