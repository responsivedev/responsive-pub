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

package dev.responsive.kafka.internal.clients;

import java.util.BitSet;

/**
 * This class allows us to efficiently count the number of events
 * between two offsets that match a certain condition. This is somewhat
 * memory efficient in that we can track 100K offsets with ~1.5K longs
 * (64 bits per long), or roughly 12KB.
 */
public class OffsetTracker {

  private BitSet offsets = new BitSet();
  private long baseOffset;

  public OffsetTracker(long baseOffset) {
    this.baseOffset = baseOffset;
  }

  public void mark(final long offset) {
    if (offset < baseOffset) {
      throw new IllegalArgumentException(
          "Offset " + offset + " cannot be less than baseOffset " + baseOffset);
    }

    // assume that we won't be committing more than MAX_INT offsets
    // in a single commit...
    final int idx = Math.toIntExact(offset - baseOffset);
    offsets.set(idx);
  }

  public int countAndShift(final long commitOffset) {
    if (commitOffset < baseOffset) {
      throw new IllegalArgumentException("Commit offset " + commitOffset + " cannot be less than baseOffset " + baseOffset);
    }

    final int shift = Math.toIntExact(commitOffset - baseOffset);
    final int count = offsets.get(0, shift).cardinality();

    // shift the bitset so that commitOffset is the new base offset
    // for which we track marked entries
    final var old = this.offsets;
    this.offsets = new BitSet();
    for (int i = old.nextSetBit(shift); i != -1; i = old.nextSetBit(i + 1)) {
      this.offsets.set(i - shift);
    }

    baseOffset = commitOffset;
    return count;
  }
}
