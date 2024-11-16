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

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RemoteWindowedTable<S> extends RemoteTable<WindowedKey, S> {

  /**
   * Initializes the table by setting the metadata fields to
   * their initialized values.
   *
   * @return a {@link WindowFlushManager} that gives the callee access
   * to run statements on {@code table}
   */
  WindowFlushManager init(
      final int kafkaPartition
  );

  /**
   * Retrieves the value of the given {@code kafkaPartition} and {@code key}.
   *
   * @param kafkaPartition  the kafka partition
   * @param key             the data key
   * @param windowStart     the start time of the window
   *
   * @return the value previously set
   */
  byte[] fetch(
      int kafkaPartition,
      Bytes key,
      long windowStart
  );

  /**
   * Retrieves the range of windows of the given {@code kafkaPartition} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo}.
   *
   * @param kafkaPartition the kafka partition
   * @param key            the data key
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a forwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> fetch(
      int kafkaPartition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  /**
   * Retrieves the range of windows of the given {@code kafkaPartition} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo}.
   *
   * @param kafkaPartition the kafka partition
   * @param key            the data key
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a backwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> backFetch(
      int kafkaPartition,
      Bytes key,
      long timeFrom,
      long timeTo
  );

  /**
   * Retrieves the range of windows of the given {@code kafkaPartition} for all keys
   * between {@code fromKey} and {@code toKey} with a start time between {@code timeFrom}
   * and {@code timeTo}.
   *
   * @param kafkaPartition the kafka partition
   * @param fromKey        the min data key (inclusive)
   * @param toKey          the max data key (inclusive)
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a forwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> fetchRange(
      int kafkaPartition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  /**
   * Retrieves the range of windows of the given {@code kafkaPartition} for all keys
   * between {@code fromKey} and {@code toKey} with a start time between {@code timeFrom}
   * and {@code timeTo}.
   *
   * @param kafkaPartition the kafka partition
   * @param fromKey        the min data key (inclusive)
   * @param toKey          the max data key (inclusive)
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a backwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      int kafkaPartition,
      Bytes fromKey,
      Bytes toKey,
      long timeFrom,
      long timeTo
  );

  /**
   * Retrieves the windows of the given {@code kafkaPartition} across all keys and with a
   * start time between {@code timeFrom} and {@code timeTo}.
   *
   * @param kafkaPartition the partition
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a forwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> fetchAll(
      int kafkaPartition,
      long timeFrom,
      long timeTo
  );

  /**
   * Retrieves the windows of the given {@code kafkaPartition} across all keys and with a
   * start time between {@code timeFrom} and {@code timeTo}.
   *
   * @param kafkaPartition the partition
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (inclusive)
   *
   * @return a backwards iterator over the retrieved windows and values previously set
   */
  KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      int kafkaPartition,
      long timeFrom,
      long timeTo
  );
}
