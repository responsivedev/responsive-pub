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

public interface TablePartitioner<K, P> {

  /**
   * @param kafkaPartition  the original partition in kafka
   * @param key             the data key
   * @return                the remote table partition
   */
  P tablePartition(final int kafkaPartition, final K key);

  /**
   * @param kafkaPartition  the original partition in kafka
   * @return                the remote table partition for metadata rows
   */
  P metadataTablePartition(final int kafkaPartition);

  static <K> TablePartitioner<K, Integer> defaultPartitioner() {
    return new DefaultPartitioner<>();
  }

  /**
   * A default, no-op partitioner that is one-to-one with the Kafka partition
   * and has only a single dimension to the partitioning key
   *
   * @param <K> the record key type
   */
  class DefaultPartitioner<K> implements TablePartitioner<K, Integer> {

    @Override
    public Integer tablePartition(final int kafkaPartition, final K key) {
      return kafkaPartition;
    }

    @Override
    public Integer metadataTablePartition(final int kafkaPartition) {
      return kafkaPartition;
    }

  }
}
