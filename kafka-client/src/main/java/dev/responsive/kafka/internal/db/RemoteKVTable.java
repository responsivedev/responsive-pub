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

package dev.responsive.kafka.internal.db;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RemoteKVTable<S> extends RemoteTable<Bytes, S> {

  /**
   * Initializes the table by setting the metadata fields to
   * their initialized values.
   *
   * @return a {@link KVFlushManager} that gives the callee access
   * to run statements on {@code table}
   */
  KVFlushManager init(
      final int kafkaPartition
  );

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param kafkaPartition  the kafka partition
   * @param key             the data key
   * @param minValidTs      the minimum valid timestamp to apply semantic TTL,
   *                        in epochMillis
   *
   * @return the value previously set
   */
  byte[] get(int kafkaPartition, Bytes key, long minValidTs);

  /**
   * Retrieves a range of key value pairs from the given {@code partitionKey} and
   * {@code table} such that the keys (compared lexicographically) fall within the
   * range of {@code from} to {@code to}.
   *
   * <p>Note that the returned iterator returns values from the remote server
   * as it's iterated (data fetching is handling by the underlying Cassandra
   * session).
   *
   * @param kafkaPartition  the kafka partition
   * @param from            the starting key (inclusive)
   * @param to              the ending key (exclusive)
   * @param minValidTs      the minimum timestamp, in epochMillis, to consider valid
   *
   * @return an iterator of all key-value pairs in the range
   */
  KeyValueIterator<Bytes, byte[]> range(
      int kafkaPartition,
      Bytes from,
      Bytes to,
      long minValidTs
  );

  /**
   * Retrieves all key value pairs from the given {@code partitionKey} and
   * {@code table} such that the keys are sorted lexicographically
   *
   * <p>Note that the returned iterator returns values from the remote server
   * as it's iterated (data fetching is handling by the underlying Cassandra
   * session).
   *
   * @param kafkaPartition  the kafka partition
   * @param minValidTs      the minimum valid timestamp, in epochMilliis, to return
   *
   * @return an iterator of all key-value pairs
   */
  KeyValueIterator<Bytes, byte[]> all(int kafkaPartition, long minValidTs);

  /**
   *  An approximate count of the total number of entries across all sub-partitions
   *  in the remote table that correspond to this kafka partition
   *
   * @param kafkaPartition the kafka partition to count entries for
   * @return the approximate number of entries for this kafka partition
   */
  long approximateNumEntries(int kafkaPartition);
}
