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

package dev.responsive.kafka.internal.db.partitioning;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.SUBPARTITION_HASHER_CONFIG;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;

/**
 * {@code SubPartitioner} allows sub-partitioning a partition
 * space into additional partitions such that there is a 1:N
 * mapping of original partitions to new partitions, aka
 * "sub-partitions". If two events are in the same subpartition
 * they must have been in the same original kafka partition
 * (though the opposite is not true).
 * <p>
 * In this context, the resulting new table partition in Cassandra
 * is referred to as the "sub-partition", whereas the kafka partition
 * may be referred to as such, or sometimes "k-partition" or the
 * "original partition"
 * <p>
 * The algorithm used simply maps each kafka partition to
 * {@code n} new subpartitions, beginning with {@code k-partition * n}
 * and ending with {@code k-partition * n + n} (exclusive). To
 * compute which new partition a key falls into, it applies a hash
 * function and mods by {@code n}, adding that value to the base
 * mapped partition.
 */
public class SubPartitioner implements TablePartitioner<Bytes, Integer> {

  public static final SubPartitioner NO_SUBPARTITIONS = new SubPartitioner(1, k -> 0);

  /**
   * the number of subpartitions is: {@code kafka_partitions * n}
   */
  private final int factor;
  private final Function<Bytes, Integer> hasher;

  public static SubPartitioner create(
      final OptionalInt actualRemoteCount,
      final int numKafkaPartitions,
      final String tableName,
      final ResponsiveConfig config,
      final String changelogTopicName
  ) {
    final int requestedNumSubPartitions = config.getInt(STORAGE_DESIRED_NUM_PARTITION_CONFIG);

    final int factor = (requestedNumSubPartitions == ResponsiveConfig.NO_SUBPARTITIONS)
        ? 1 : (int) Math.ceil((double) requestedNumSubPartitions / numKafkaPartitions);
    final int computedRemoteNum = factor * numKafkaPartitions;

    if (actualRemoteCount.isPresent() && actualRemoteCount.getAsInt() != computedRemoteNum) {
      throw new ConfigException(
          String.format(
              "%s was configured to %d, which "
                  + "given %s partitions in kafka topic %s would result in %d remote partitions "
                  + "for table %s (remote partitions must be a multiple of the kafka partitions). "
                  + "The remote store is already initialized with %d partitions - it is backwards "
                  + "incompatible to change this. Please set %s to %d.",
              STORAGE_DESIRED_NUM_PARTITION_CONFIG, requestedNumSubPartitions, numKafkaPartitions,
              changelogTopicName, computedRemoteNum, tableName,
              actualRemoteCount.getAsInt(), STORAGE_DESIRED_NUM_PARTITION_CONFIG,
              actualRemoteCount.getAsInt()));
    }

    final var hasher = config.getConfiguredInstance(SUBPARTITION_HASHER_CONFIG, Hasher.class);
    return new SubPartitioner(factor, hasher);
  }

  SubPartitioner(final int factor, final Function<Bytes, Integer> hasher) {
    this.factor = factor;
    this.hasher = hasher;
  }

  @Override
  public Integer tablePartition(final int kafkaPartition, final Bytes key) {
    return first(kafkaPartition) + Utils.toPositive(hasher.apply(key)) % factor;
  }

  @Override
  public Integer metadataTablePartition(final int kafkaPartition) {
    return first(kafkaPartition);
  }

  /**
   * @param kafkaPartition the original kafka partition
   *
   * @return all subpartitions that this partition could map to
   */
  public List<Integer> allTablePartitions(final int kafkaPartition) {
    return IntStream
        .range(first(kafkaPartition), first(kafkaPartition) + factor)
        .boxed()
        .collect(Collectors.toList());
  }

  /**
   * @return the multiplication factor of this sub-partitioner
   */
  public int getFactor() {
    return factor;
  }

  /**
   * @param kafkaPartition the original kafka partition\\
   * @return the sub-partition at index 0 for this kafka partition
   */
  private int first(final int kafkaPartition) {
    return kafkaPartition * factor;
  }

}
