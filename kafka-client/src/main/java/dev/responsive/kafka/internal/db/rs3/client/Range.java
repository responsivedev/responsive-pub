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

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Objects;

public class Range<K extends Comparable<K>> {
  private final RangeBound<K> start;
  private final RangeBound<K> end;

  public Range(RangeBound<K> start, RangeBound<K> end) {
    this.start = start;
    this.end = end;
  }

  public RangeBound<K> start() {
    return start;
  }

  public RangeBound<K> end() {
    return end;
  }

  public boolean contains(K key) {
    return greaterThanStartBound(key) && lessThanEndBound(key);
  }

  public boolean greaterThanStartBound(K key) {
    return start.map(new RangeBound.Mapper<>() {
      @Override
      public Boolean map(final RangeBound.InclusiveBound<K> b) {
        return b.key().compareTo(key) <= 0;
      }

      @Override
      public Boolean map(final RangeBound.ExclusiveBound<K> b) {
        return b.key().compareTo(key) < 0;
      }

      @Override
      public Boolean map(final RangeBound.Unbounded<K> b) {
        return true;
      }
    });
  }

  public boolean lessThanEndBound(K key) {
    return end.map(new RangeBound.Mapper<>() {
      @Override
      public Boolean map(final RangeBound.InclusiveBound<K> b) {
        return b.key().compareTo(key) >= 0;
      }

      @Override
      public Boolean map(final RangeBound.ExclusiveBound<K> b) {
        return b.key().compareTo(key) > 0;
      }

      @Override
      public Boolean map(final RangeBound.Unbounded<K> b) {
        return true;
      }
    });
  }

  public static <T extends Comparable<T>> Range<T> unbounded() {
    return new Range<>(RangeBound.unbounded(), RangeBound.unbounded());
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
