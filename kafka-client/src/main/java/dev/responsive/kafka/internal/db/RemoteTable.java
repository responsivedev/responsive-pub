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

import dev.responsive.kafka.internal.utils.WindowedKey;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;

/**
 * @param <K> the key type, e.g. {@link Bytes} or {@link WindowedKey}
 * @param <S> the write statement type, for adding updates to a write batch
 */
public interface RemoteTable<K, S> {

  String name();

  /**
   * Inserts data into {@code table}. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param kafkaPartition  the kafka partition
   * @param key             the data key
   * @param value           the data value
   * @param epochMillis     the event time with which this event
   *                      was inserted in epochMillis
   *
   * @return a statement that, when executed, will insert the entry
   *         corresponding to the given {@code kafkaPartition} and
   *         {@code key} to this {@code table} with value {@code value}
   */
  @CheckReturnValue
  S insert(
      final int kafkaPartition,
      final K key,
      final byte[] value,
      final long epochMillis
  );

  /**
   * @param kafkaPartition  the kafka partition
   * @param key             the data key
   *
   * @return a statement that, when executed, will delete the entry
   *         corresponding to the given {@code kafkaPartition} and
   *         {@code key} in this {@code table}
   */
  @CheckReturnValue
  S delete(
      final int kafkaPartition,
      final K key
  );

  /**
   * @param kafkaPartition the kafka partition
   * @return the current offset fetched from the metadata table
   *         partition for the given kafka partition
   */
  long fetchOffset(final int kafkaPartition);
}
