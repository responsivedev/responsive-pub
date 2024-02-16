package dev.responsive.kafka.internal.db;

import java.sql.Time;

public interface TimePartitionableKey<T> extends Comparable<T> {
  /**
   * @return the timestamp that partitioners can use to partition
   *         keys of this type into different segments.
   */
  long getPartitionTimestamp();
}
