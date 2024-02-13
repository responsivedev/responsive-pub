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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class WindowStoreStub {
  final Comparator<WindowedKey> keyComparator = Comparator.comparing(k -> k.key);

  private final NavigableMap<WindowedKey, byte[]> records =
      new TreeMap<>(keyComparator.thenComparingLong(k -> k.windowStartMs));

  private final long retentionPeriod;
  private long observedStreamTime = 0L;

  public WindowStoreStub() {
    // TODO: how can we pass the actual retention period through to the store stub?
    this.retentionPeriod = 15L;
  }

  public void put(final WindowedKey key, final byte[] value) {
    observedStreamTime = Math.max(observedStreamTime, key.windowStartMs);
    records.put(key, value);
  }

  public void delete(final WindowedKey key) {
    observedStreamTime = Math.max(observedStreamTime, key.windowStartMs);
    records.remove(key);
  }

  public byte[] fetch(
      final Bytes key,
      final long windowStart
  ) {
    final WindowedKey windowedKey = new WindowedKey(key, windowStart);
    if (windowStart < minValidTimestamp() && records.containsKey(windowedKey)) {
      return records.get(windowedKey);
    } else {
      return null;
    }
  }

  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final Bytes key,
      long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> all() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<WindowedKey, byte[]> backAll() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  private long minValidTimestamp() {
    return observedStreamTime - retentionPeriod + 1;
  }
}
