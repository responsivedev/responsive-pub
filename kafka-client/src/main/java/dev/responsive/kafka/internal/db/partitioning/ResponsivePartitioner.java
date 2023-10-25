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

public interface ResponsivePartitioner<K, P> {

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

  static <K> ResponsivePartitioner<K, Integer> defaultPartitioner() {
    return new DefaultPartitioner<>();
  }

  /**
   * A default, no-op partitioner that is one-to-one with the Kafka partition
   * and has only a single dimension to the partitioning key
   *
   * @param <K> the record key type
   */
  class DefaultPartitioner<K> implements ResponsivePartitioner<K, Integer> {

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
