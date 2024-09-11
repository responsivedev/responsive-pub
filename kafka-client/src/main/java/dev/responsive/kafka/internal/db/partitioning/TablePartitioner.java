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

import org.apache.kafka.common.utils.Bytes;

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

  /**
   * @param key             the data key
   * @param kafkaPartition  the partition in kafka
   * @return whether {@code key} belongs in {@code kafkaPartition}
   */
  boolean belongs(final Bytes key, final int kafkaPartition);

  static <K> TablePartitioner<K, Integer> defaultPartitioner(final int numPartitions) {
    return new DefaultPartitioner<>(numPartitions);
  }

}
