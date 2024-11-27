/*
 * Copyright 2024 Responsive Computing, Inc.
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
