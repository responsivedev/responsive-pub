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

import java.util.Comparator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KeySpec<K> extends Comparator<K> {

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
