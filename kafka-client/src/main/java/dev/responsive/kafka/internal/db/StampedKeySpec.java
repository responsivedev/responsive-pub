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

import dev.responsive.kafka.internal.utils.Stamped;
import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

public class StampedKeySpec implements KeySpec<Stamped> {

  private final Predicate<Stamped> withinRetention;

  public StampedKeySpec(final Predicate<Stamped> withinRetention) {
    this.withinRetention = withinRetention;
  }

  @Override
  public int sizeInBytes(final Stamped key) {
    return key.key.get().length + Long.BYTES;
  }

  @Override
  public Stamped keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();
    final byte[] val = record.value();

    // the timestamp is encoded in the value as the first 8 bytes
    final ByteBuffer buffer = ByteBuffer.wrap(val);
    final long startTs = buffer.getLong(0);

    return new Stamped(Bytes.wrap(key), startTs);
  }

  @Override
  public boolean retain(final Stamped key) {
    return withinRetention.test(key);
  }
}
