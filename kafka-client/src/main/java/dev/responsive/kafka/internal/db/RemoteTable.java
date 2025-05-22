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
   * @param timestampMs     the timestamp of the event being processed that
   *                        triggered this write
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
      final long timestampMs
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
   * Get the offset corresponding to the last write to the table for a specific
   * Kafka partition. This offset is exclusive (one more than the offset of
   * the last processed record).
   *
   * @param kafkaPartition the kafka partition
   * @return the last written offset (exclusive) from the table for
   *   the given kafka partition
   */
  long lastWrittenOffset(final int kafkaPartition);
}
