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

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.utils.Bytes;

/**
 * A default, no-op partitioner that is one-to-one with the Kafka partition
 * and has only a single dimension to the partitioning key
 *
 * @param <K> the record key type
 */
public class DefaultPartitioner<K> implements TablePartitioner<K, Integer> {

  private final int numPartitions;

  public DefaultPartitioner(final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  @Override
  public Integer tablePartition(final int kafkaPartition, final K key) {
    return kafkaPartition;
  }

  @Override
  public Integer metadataTablePartition(final int kafkaPartition) {
    return kafkaPartition;
  }

  @Override
  public boolean belongs(final Bytes key, final int kafkaPartition) {
    return BuiltInPartitioner.partitionForKey(key.get(), numPartitions) == kafkaPartition;
  }

}
