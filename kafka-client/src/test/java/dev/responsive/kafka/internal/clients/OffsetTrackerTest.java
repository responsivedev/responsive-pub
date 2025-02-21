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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class OffsetTrackerTest {

  @Test
  public void shouldMarkAndCountSingleOffset() {
    // Given
    final OffsetTracker tracker = new OffsetTracker(1000L);
    tracker.mark(1005L);

    // When
    int count = tracker.countAndShift(1010L);

    // Then
    assertThat("Expected one marked offset between 1000 and 1010", count, is(equalTo(1)));
  }

  @Test
  public void shouldMarkAndCountMultipleOffsetsAndIgnoreHigherOffsets() {
    // Given
    final OffsetTracker tracker = new OffsetTracker(1000L);
    tracker.mark(1001L);
    tracker.mark(1003L);
    tracker.mark(1005L);
    tracker.mark(1009L);

    // When
    int count = tracker.countAndShift(1006L);

    // Then
    assertThat("Expected three marked offsets before 1006", count, is(equalTo(3)));
  }

  @Test
  public void shouldCountNoMarkedOffsets() {
    // Given: no marks have been set
    final OffsetTracker tracker = new OffsetTracker(1000L);

    // When
    int count = tracker.countAndShift(1010L);

    // Then
    assertThat("Expected no marks when none have been set", count, is(equalTo(0)));
  }

  @Test
  public void shouldThrowExceptionOnMarkingOldOffset() {
    // Given:
    final OffsetTracker tracker = new OffsetTracker(1000L);

    // When/Then: attempting to mark offset 999 should throw an IllegalArgumentException
    Exception ex = assertThrows(IllegalArgumentException.class, () -> tracker.mark(999L));
    assertThat(ex.getMessage(), containsString("cannot be less than baseOffset"));
  }

  @Test
  public void shouldThrowWhenCommittingOldOffset() {
    // Given:
    final OffsetTracker tracker = new OffsetTracker(1000L);

    // When/Then: committing at an offset below the base should throw an exception
    Exception ex = assertThrows(IllegalArgumentException.class, () -> tracker.countAndShift(999L));
    assertThat(ex.getMessage(), containsString("cannot be less than baseOffset"));
  }

  @Test
  public void shouldShiftAndMarkAppropriately() {
    // Given:
    final OffsetTracker tracker = new OffsetTracker(1000L);
    tracker.mark(1001L);
    tracker.mark(1004L);

    // When:
    tracker.countAndShift(1005L);
    tracker.mark(1006L); // becomes index 1 relative to new base 1005
    tracker.mark(1007L); // becomes index 2 relative to base 1005
    tracker.mark(1011L); // out of range for next commit
    int count = tracker.countAndShift(1010L);

    // Then:
    assertThat("Expected two marks before 1010", count, is(equalTo(2)));
  }

}