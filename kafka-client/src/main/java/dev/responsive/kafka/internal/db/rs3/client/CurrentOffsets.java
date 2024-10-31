package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Optional;

public class CurrentOffsets {
  /**
   * Represents the last offset that was written to the PSS for a given LSS. This offset
   * may not be durably flushed yet.
   */
  private final Optional<Long> writtenOffset;
  /**
   * Represents the last offset that was durably flushed to the PSS for a given LSS.
   */
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
