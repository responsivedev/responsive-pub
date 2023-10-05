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

public interface RemoteKVTable extends RemoteTable<Bytes> {

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key}
   * from {@code table}.
   *
   * @param partition  the partition
   * @param key        the data key
   * @param minValidTs the minimum valid timestamp to apply semantic TTL,
   *                   in epochMillis
   *
   * @return the value previously set
   */
  byte[] get(int partition, Bytes key, long minValidTs);

  /**
   * Retrieves a range of key value pairs from the given {@code partitionKey} and
   * {@code table} such that the keys (compared lexicographically) fall within the
   * range of {@code from} to {@code to}.
   *
   * <p>Note that the returned iterator returns values from the remote server
   * as it's iterated (data fetching is handling by the underlying Cassandra
   * session).
   *
   * @param partition  the partition
   * @param from       the starting key (inclusive)
   * @param to         the ending key (exclusive)
   * @param minValidTs the minimum timestamp, in epochMillis, to consider
   *                   valid
   *
   * @return an iterator of all key-value pairs in the range
   */
  KeyValueIterator<Bytes, byte[]> range(
      int partition,
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
   * @param partition  the partition
   * @param minValidTs the minimum valid timestamp, in epochMilliis, to return
   *
   * @return an iterator of all key-value pairs
   */
  KeyValueIterator<Bytes, byte[]> all(int partition, long minValidTs);

}
