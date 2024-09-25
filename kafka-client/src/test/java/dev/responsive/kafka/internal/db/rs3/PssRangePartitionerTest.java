package dev.responsive.kafka.internal.db.rs3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.junit.jupiter.api.Test;

class PssRangePartitionerTest {
  private static final List<String> PREFIXES = List.of("000", "001", "010", "011", "1");

  private final PssRangePartitioner partitioner = PssRangePartitioner.create(
      PREFIXES,
      key -> ((int) key[0]) << 24
  );
  private final PssRangePartitioner realPartitioner = PssRangePartitioner.create(PREFIXES);

  @Test
  public void shouldComputePssIdsCorrectly() {
    // when:
    final var allPss = partitioner.allPss();

    // then:
    assertThat(allPss, containsInAnyOrder(8, 9, 10, 11, 3));
  }

  @Test
  public void shouldAssignToPssCorrectly() {
    final Queue<Byte> prefixBytes
        = new LinkedList<>(List.of((byte) 0b0, (byte) 0b100000, (byte) 0b1000000, (byte) 0b1100000));
    final Queue<Integer> expectedPssList = new LinkedList<>(List.of(8, 9, 10, 11));
    byte testCase = Byte.MIN_VALUE;
    int expectedPss = 3;
    while (true) {
      if (prefixBytes.peek() != null && prefixBytes.peek() == testCase) {
        prefixBytes.poll();
        expectedPss = expectedPssList.poll();
      }

      // when:
      final int pss = partitioner.pss(new byte[]{testCase}, 0);

      // then:
      assertThat(pss, equalTo(expectedPss));

      if (testCase == Byte.MAX_VALUE) {
        break;
      }
      testCase++;
    }
  }

  @Test
  public void shouldAssignToPssCorrectlyUsingRealHash() {
    // given:
    final byte[] key = "test key".getBytes();

    // when:
    final int pss = realPartitioner.pss(key, 0);

    // then:
    assertThat(pss, is(3));
  }
}