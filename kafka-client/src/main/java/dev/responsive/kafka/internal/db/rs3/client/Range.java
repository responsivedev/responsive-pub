package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Arrays;
import java.util.Objects;

public class Range {
  private final RangeBound start;
  private final RangeBound end;

  public Range(RangeBound start, RangeBound end) {
    this.start = start;
    this.end = end;
  }

  public RangeBound start() {
    return start;
  }

  public RangeBound end() {
    return end;
  }

  public boolean contains(byte[] key) {
    return greaterThanStartBound(key) && lessThanEndBound(key);
  }

  public boolean greaterThanStartBound(byte[] key) {
    return start.map(new RangeBound.Mapper<>() {
      @Override
      public Boolean map(final RangeBound.InclusiveBound b) {
        return Arrays.compare(b.key(), key) <= 0;
      }

      @Override
      public Boolean map(final RangeBound.ExclusiveBound b) {
        return Arrays.compare(b.key(), key) < 0;
      }

      @Override
      public Boolean map(final RangeBound.Unbounded b) {
        return true;
      }
    });
  }

  public boolean lessThanEndBound(byte[] key) {
    return end.map(new RangeBound.Mapper<>() {
      @Override
      public Boolean map(final RangeBound.InclusiveBound b) {
        return Arrays.compare(b.key(), key) >= 0;
      }

      @Override
      public Boolean map(final RangeBound.ExclusiveBound b) {
        return Arrays.compare(b.key(), key) > 0;
      }

      @Override
      public Boolean map(final RangeBound.Unbounded b) {
        return true;
      }
    });
  }

  public static Range unbounded() {
    return new Range(RangeBound.unbounded(), RangeBound.unbounded());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Range range = (Range) o;
    return Objects.equals(start, range.start) && Objects.equals(end, range.end);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }
}
