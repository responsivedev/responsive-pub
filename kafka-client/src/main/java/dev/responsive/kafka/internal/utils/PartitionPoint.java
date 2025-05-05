package dev.responsive.kafka.internal.utils;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public final class PartitionPoint {

  public static <T> Optional<Integer> partitionPoint(
      final List<T> items,
      final Function<T, Boolean> predicate
  ) {
    int low = 0;
    int high = items.size() - 1;
    int partition_point = 0;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      if (!predicate.apply(items.get(mid))) {
        low = mid + 1;
        partition_point = mid + 1;
      } else if (mid > low) {
        high = mid - 1;
      } else {
        break;
      }
    }
    if (partition_point == items.size()) {
      return Optional.empty();
    }
    return Optional.of(partition_point);
  }
}
