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

package dev.responsive.kafka.internal.clients;

import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows us to efficiently count the number of events
 * between two offsets that match a certain condition. This is somewhat
 * memory efficient in that we can track 100K offsets with ~1.5K longs
 * (64 bits per long), or roughly 12KB.
 * <p>
 * This is necessary for origin event tracking: since we mark records
 * on poll but report aligned to commit it is possible that there are
 * records that are polled but not included in the committed offsets.
 * This class remedies that by allowing us to count the marked events
 * up to the committed offset.
 * <p>
 * Additionally, there are some situations that would cause us to poll
 * a record multiple times (e.g. a task is reassigned to the same thread
 * after an EOS error). This class is idempotent to marking the same
 * offsets multiple times.
 */
public class OffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(OffsetTracker.class);

  private BitSet offsets = new BitSet();
  private long baseOffset;

  public OffsetTracker(long baseOffset) {
    this.baseOffset = baseOffset;
  }

  public void mark(final long offset) {
    if (offset < baseOffset) {
      LOG.error("Invalid offset {} lower than baseOffset {} marked", offset, baseOffset);
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
      LOG.error("Invalid offset {} lower than baseOffset {} committed", commitOffset, baseOffset);
      throw new IllegalArgumentException(
          "Commit offset " + commitOffset + " cannot be less than baseOffset " + baseOffset);
    }

    final int shift = Math.toIntExact(commitOffset - baseOffset);
    final int count = offsets.get(0, shift).cardinality();

    // shift the bitset so that commitOffset is the new base offset
    // for which we track marked entries
    if (shift < offsets.length()) {
      offsets = offsets.get(shift, offsets.length());
    } else {
      // if the shift is beyond the last entry in the bitset, we can
      // just create a new empty bitset
      offsets = new BitSet();
    }

    baseOffset = commitOffset;
    return count;
  }
}
