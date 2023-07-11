/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.utils;

import dev.responsive.kafka.config.ResponsiveConfig;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.internals.Murmur3;

/**
 * {@code SubPartitioner} allows sub-partitioning a partition
 * space into additional partitions such that there is a 1:N
 * mapping of original partitions to new partitions (namely,
 * if two events are in the same new partition they must have
 * been in the same original partition, but the opposite is
 * not true).
 *
 * <p>The algorithm used simply maps each original partition
 * to {@code n} new partitions, beginning with {@code original
 * * n} and ending with {@code original * n + n} (exclusive). To
 * compute which new partition a key falls into, it applies a hash
 * function and mods by {@code n}, adding that value to the base
 * mapped partition.</p>
 */
public class SubPartitioner {

  public static final SubPartitioner NO_SUBPARTITIONS = new SubPartitioner(1, k -> 0);

  // ensure that the default sub-partitioning hasher is unlikely
  // to be the same as the hasher that was used for the original
  // partition scheme - if the hashers are the same, and the
  // number of sub partitions is equal to the number of original
  // partitions, all keys would be mapped to the same sub-partition
  // and all other sub-partitions would be empty
  static final int SALT = 31;

  /**
   * the number of subpartitions is: {@code original_partitions * n}
   */
  private final int factor;
  private final Function<Bytes, Integer> hasher;

  public static SubPartitioner create(
      final OptionalInt actualRemoteCount,
      final int kafkaPartitions,
      final int desiredNum,
      final TableName name,
      final String changelogTopicName
  ) {
    final int factor = (desiredNum == ResponsiveConfig.NO_SUBPARTITIONS) ? 1 : desiredNum / kafkaPartitions;
    final int computedRemoteNum = factor * kafkaPartitions;

    if (actualRemoteCount.isPresent() && actualRemoteCount.getAsInt() != computedRemoteNum) {
      throw new ConfigException(String.format("%s was configured to %d, which "
              + "given %s partitions in kafka topic %s would result in %d remote partitions "
              + "for table %s (remote partitions must be a multiple of the kafka partitions). "
              + "The remote store is already initialized with %d partitions - it is backwards "
              + "incompatible to change this. Please set %s to %d.",
          ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG, desiredNum, kafkaPartitions,
          changelogTopicName, computedRemoteNum, name.kafkaName(),
          actualRemoteCount.getAsInt(), ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG,
          actualRemoteCount.getAsInt()));
    }

    return new SubPartitioner(factor);
  }

  public SubPartitioner(final int factor) {
    this(factor, k -> SALT * Murmur3.hash32(k.get()));
  }

  public SubPartitioner(final int factor, final Function<Bytes, Integer> hasher) {
    this.factor = factor;
    this.hasher = hasher;
  }

  public int partition(final int original, final Bytes key) {
    return first(original) + Utils.toPositive(hasher.apply(key)) % factor;
  }

  /**
   * @return the multiplication factor of this sub-partitioner
   */
  public int getFactor() {
    return factor;
  }

  /**
   * @param partition the original partition
   * @return the sub-partition at index 0 for this partition
   */
  public int first(final int partition) {
    return partition * factor;
  }

  /**
   * @param partition the original partition
   * @return all subpartitions that this partition could map to
   */
  public IntStream all(final int partition) {
    return IntStream.range(first(partition), first(partition) + factor);
  }
}
