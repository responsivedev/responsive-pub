/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db.partitioning;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.SUBPARTITION_HASHER_CONFIG;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
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
    final int requestedNumSubPartitions = config.getInt(CASSANDRA_DESIRED_NUM_PARTITION_CONFIG);

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
              CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, requestedNumSubPartitions, numKafkaPartitions,
              changelogTopicName, computedRemoteNum, tableName,
              actualRemoteCount.getAsInt(), CASSANDRA_DESIRED_NUM_PARTITION_CONFIG,
              actualRemoteCount.getAsInt()));
    }

    final var hasher = config.getConfiguredInstance(SUBPARTITION_HASHER_CONFIG, Hasher.class);
    return new SubPartitioner(factor, hasher);
  }

  @VisibleForTesting
  public SubPartitioner(final int factor, final Function<Bytes, Integer> hasher) {
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
