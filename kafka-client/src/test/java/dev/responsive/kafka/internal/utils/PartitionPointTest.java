package dev.responsive.kafka.internal.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import static dev.responsive.kafka.internal.utils.PartitionPoint.partitionPoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PartitionPointTest {
  @Test
  public void shouldComputePartitionPointOfEmptyList() {
    assertThat(partitionPoint(new ArrayList<Boolean>(), v -> v), is(Optional.empty()));
  }

  @Test
  public void shouldComputePartitionPoint() {
    for (int len = 1; len < 10; len++) {
      final List<Boolean> allFalse = new ArrayList<>(Collections.nCopies(len, false));
      assertThat(partitionPoint(allFalse, v -> v), is(Optional.empty()));
      for (int p = 0; p < len; p++) {
        final List<Boolean> items = new ArrayList<>(allFalse);
        for (int i = p; i < len; i++) {
          items.set(i, true);
        }
        assertThat(partitionPoint(items, v -> v), is(Optional.of(p)));
      }
    }
  }
}