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

import dev.responsive.kafka.internal.utils.SessionKey;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SessionKeySpec implements KeySpec<SessionKey> {

  private final Predicate<SessionKey> withinRetention;

  public SessionKeySpec(final Predicate<SessionKey> withinRetention) {
    this.withinRetention = withinRetention;
  }

  @Override
  public int sizeInBytes(final SessionKey key) {
    return key.key.get().length + Long.BYTES;
  }

  @Override
  public SessionKey keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();

    // TODO(antoine): IMPLEMENT
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public boolean retain(final SessionKey key) {
    return withinRetention.test(key);
  }
}
