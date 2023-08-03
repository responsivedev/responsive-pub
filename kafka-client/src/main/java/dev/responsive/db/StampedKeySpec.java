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

package dev.responsive.db;

import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;

import dev.responsive.kafka.store.ResponsiveWindowStore;
import dev.responsive.model.Stamped;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

public class StampedKeySpec implements KeySpec<Stamped<Bytes>> {

  private final Predicate<Stamped<Bytes>> withinRetention;

  public StampedKeySpec(final Predicate<Stamped<Bytes>> withinRetention) {
    this.withinRetention = withinRetention;
  }

  @Override
  public Bytes bytes(final Stamped<Bytes> key) {
    return key.key;
  }

  @Override
  public Stamped<Bytes> keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
    final byte[] key = record.key();
    final int size = key.length - TIMESTAMP_SIZE;

    final ByteBuffer buffer = ByteBuffer.wrap(key);
    final long startTs = buffer.getLong(size);
    final Bytes kBytes = Bytes.wrap(Arrays.copyOfRange(key, 0, size));

    return new Stamped<>(kBytes, startTs);
  }

  @Override
  public boolean retain(final Stamped<Bytes> key) {
    return withinRetention.test(key);
  }

  @Override
  public int compare(final Stamped<Bytes> o1, final Stamped<Bytes> o2) {
    return ResponsiveWindowStore.compareKeys(o1, o2);
  }
}
