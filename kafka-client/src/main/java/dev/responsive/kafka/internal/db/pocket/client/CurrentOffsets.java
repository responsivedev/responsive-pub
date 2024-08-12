package dev.responsive.kafka.internal.db.pocket.client;

public class CurrentOffsets {
  private final long writtenOffset;
  private final long flushedOffset;

  public CurrentOffsets(final long writtenOffset, final long flushedOffset) {
    this.writtenOffset = writtenOffset;
    this.flushedOffset = flushedOffset;
  }

  public long writtenOffset() {
    return writtenOffset;
  }

  public long flushedOffset() {
    return flushedOffset;
  }
}
