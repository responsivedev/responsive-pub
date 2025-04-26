package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Map;
import java.util.Optional;

public class LssMetadata {
  private final long lastWrittenOffset;
  private final Map<Integer, Optional<Long>> writtenOffsets;

  public LssMetadata(long lastWrittenOffset, Map<Integer, Optional<Long>> writtenOffsets) {
    this.lastWrittenOffset = lastWrittenOffset;
    this.writtenOffsets = writtenOffsets;
  }

  /**
   * Get the last written offset for the LSS.
   *
   * @return The last written offset or
   *  {@link dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration#NO_COMMITTED_OFFSET}
   *  if there is none
   */
  public long lastWrittenOffset() {
    return lastWrittenOffset;
  }

  /**
   * Get the last written offset for each PSS mapped to the LSS. It may return
   * `Optional.empty()` if the PSS has no written offsets yet.
   *
   * @return a map of the last written offsets for each PSS ID
   */
  public Map<Integer, Optional<Long>> writtenOffsets() {
    return writtenOffsets;
  }

}
