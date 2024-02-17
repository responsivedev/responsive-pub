package dev.responsive.kafka.internal.db;

public interface SegmentableKey<T> extends Comparable<T> {
  /**
   * @return the timestamp that partitioners can use to partition
   *         keys of this type into different segments.
   */
  long segmentTimestamp();
}
