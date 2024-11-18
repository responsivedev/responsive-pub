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

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KeySpec<K extends Comparable<K>> {

  K keyFromRecord(final ConsumerRecord<byte[], byte[]> record);

  int sizeInBytes(final K key);

  /**
   * Used to determine whether a key is still valid. Window stores for example
   * will consider a record no longer valid if the windowed key's timestamp is
   * outside of the state store's retention period.
   *
   * @param key the key to test
   * @return true iff the key should be retained,
   *         false if it is no longer valid and should be cleaned up.
   */
  default boolean retain(final K key) {
    return true;
  }

}
