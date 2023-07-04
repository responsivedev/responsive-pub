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

import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.internals.Murmur3;

/**
 * {@code ExplodePartitioner} allows mapping a smaller subset of partitions
 * into a larger partition space.
 */
public class ExplodePartitioner {

  private static final int SALT = 31;

  private final int factor;
  private final Function<Bytes, Integer> hasher;

  public ExplodePartitioner(
      final int factor
  ) {
    // we use a salt in case the original partitioner used Murmur3.hash32, which
    // would cause all keys within a base partition to map to the same mapped partition
    this(factor, k -> SALT * Murmur3.hash32(k.get()));
  }

  public ExplodePartitioner(
      final int factor,
      final Function<Bytes, Integer> hasher
  ) {
    this.factor = factor;
    this.hasher = hasher;
  }

  public int repartition(final int original, final Bytes key) {
    // map the original partition to a new base by multiplying it by factor
    // and then determine the sub-partition within the new base by partitioning
    // it as if there were that many partitions
    return original * factor + Utils.toPositive(hasher.apply(key)) % factor;
  }

  public int getFactor() {
    return factor;
  }

  public int base(final int partition) {
    return partition * factor;
  }
}
