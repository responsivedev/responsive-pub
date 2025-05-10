/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db;

import org.apache.kafka.common.serialization.Serializer;
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
   * @param streamTimeMs    the current streamTime
   *
   * @return the value previously set
   */
  byte[] get(int kafkaPartition, Bytes key, long streamTimeMs);

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
   * @param to              the ending key (inclusive)
   * @param streamTimeMs    the current streamTime
   *
   * @return an iterator of all key-value pairs in the range
   */
  KeyValueIterator<Bytes, byte[]> range(
      int kafkaPartition,
      Bytes from,
      Bytes to,
      long streamTimeMs
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
   * @param streamTimeMs    the current streamTime
   *
   * @return an iterator of all key-value pairs
   */
  KeyValueIterator<Bytes, byte[]> all(int kafkaPartition, long streamTimeMs);

  <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefix(
      P prefix,
      PS prefixKeySerializer,
      int kafkaPartition,
      long streamTimeMs
  );

  /**
   *  An approximate count of the total number of entries across all sub-partitions
   *  in the remote table that correspond to this kafka partition
   *
   * @param kafkaPartition the kafka partition to count entries for
   * @return the approximate number of entries for this kafka partition
   */
  long approximateNumEntries(int kafkaPartition);

  default byte[] checkpoint() {
    throw new UnsupportedOperationException("checkpoints not supported for this store type");
  }
}
