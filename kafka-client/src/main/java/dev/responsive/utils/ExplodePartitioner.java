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

import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * {@code ExplodePartitioner} allows mapping a smaller subset of partitions
 * into a larger partition space.
 */
public class ExplodePartitioner<K, V> {

  private final StreamPartitioner<K, V> base;
  private final int factor;
  private final int numPartitions;
  private final String topic;

  public ExplodePartitioner(
      final StreamPartitioner<K, V> base,
      final String topic,
      final int factor,
      final int numPartitions
  ) {
    if (factor != 1 && factor == numPartitions) {
      // we don't necessarily have to use the same partitioner for the
      // second round of hashing as we do for the first one, but we'll
      // leave that as a follow-up and just throw an exception for now
      throw new IllegalArgumentException("Cannot explode by the same "
          + "number as original partitions " + numPartitions
          + " as that will ensure collisions.");
    }
    this.base = base;
    this.topic = topic;
    this.factor = factor;
    this.numPartitions = numPartitions;
  }

  @SuppressWarnings("deprecation")
  public int repartition(final K key, final V value) {
    // map the original partition to a new base by multiplying it by factor
    // and then determine the sub-partition within the new base by partitioning
    // it as if there were that many partitions
    return base.partition(topic, key, value, numPartitions) * factor
        + base.partition(topic, key, value, factor);
  }

  public int getFactor() {
    return factor;
  }

  public int mapToBase(final int partition) {
    return partition * factor;
  }
}
