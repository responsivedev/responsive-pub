package dev.responsive.examples.e2etest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class AccumulatingChecksumTest {
  @Test
  public void shouldComputeUniqueChecksumsOnScramble() {
    final int nmsgs = 100;
    final int iters = 10;
    final List<Long> vals = new ArrayList<>(LongStream.range(0, nmsgs).boxed().toList());
    final Set<ByteArray> seenDigests = new HashSet<>();
    seenDigests.add(digestFrom(vals));
    for (int i = 0; i < iters; i++) {
      Collections.shuffle(vals);
      final var digest = digestFrom(vals);
      assertThat(digest, not(in(seenDigests)));
      seenDigests.add(digest);
    }
  }

  @Test
  public void shouldComputeUniqueChecksumOnDrop() {
    final int nmsgs = 100;
    final List<Long> vals = LongStream.range(0, nmsgs).boxed().toList();
    final Set<ByteArray> seenDigests = new HashSet<>();
    seenDigests.add(digestFrom(vals));
    for (int i = 0; i < nmsgs; i++) {
      final List<Long> dropped = new LinkedList<>(vals);
      dropped.remove(i);
      final var digest = digestFrom(dropped);
      assertThat(digest, not(in(seenDigests)));
      seenDigests.add(digest);
    }
  }

  @Test
  public void shouldComputeSameChecksum() {
    final int nmsgs = 100;
    final List<Long> vals = new LinkedList<>();
    for (long l = 0; l < nmsgs; l++) {
      vals.add(l);
      assertThat(digestFrom(vals), equalTo(digestFrom(vals)));
    }
  }

  private ByteArray digestFrom(final List<Long> longs) {
    AccumulatingChecksum checksum = new AccumulatingChecksum();
    for (final var l : longs) {
      checksum = new AccumulatingChecksum(checksum.updateWith(l).current());
    }
    return new ByteArray(checksum.current());
  }

  private class ByteArray {
    private final byte[] digest;

    private ByteArray(final byte[] digest) {
      this.digest = digest;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ByteArray digest1 = (ByteArray) o;
      return Arrays.equals(digest, digest1.digest);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(digest);
    }

    @Override
    public String toString() {
      return Arrays.toString(digest);
    }
  }
}