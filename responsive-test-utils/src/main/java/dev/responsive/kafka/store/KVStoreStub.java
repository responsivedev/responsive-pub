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

package dev.responsive.kafka.store;

import dev.responsive.utils.Iterators;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class KVStoreStub {

  private final NavigableMap<Bytes, byte[]> records = new TreeMap<>();
  private final Map<Bytes, Long> expiryTimes = new LinkedHashMap<>();

  private final Duration ttl; // null if ttl not enabled
  private final Time time;

  public KVStoreStub(final Duration ttl, final Time time) {
    this.ttl = ttl;
    this.time = time;
  }

  public long count() {
    expireRecords();
    return records.size();
  }

  public void put(final Bytes key, final byte[] value) {
    expireRecords();
    if (records.containsKey(key)) {
      delete(key); // reset the order for ttl
    }
    records.put(key, value);
    expiryTimes.put(key, expirationDateMs());
  }

  public void delete(final Bytes key) {
    records.remove(key);
  }

  public byte[] get(final Bytes key) {
    expireRecords();
    return records.get(key);
  }

  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    expireRecords();
    return Iterators.kv(
        records.subMap(from, to).entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  public KeyValueIterator<Bytes, byte[]> all() {
    expireRecords();
    return Iterators.kv(
        records.entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  private void expireRecords() {
    final long now = time.milliseconds();
    final var iter = expiryTimes.entrySet().iterator();
    while (iter.hasNext()) {
      final var entry = iter.next();
      if (entry.getValue() < now) {
         iter.remove();
         records.remove(entry.getKey());
      } else {
        break;
      }
    }
  }

  private long expirationDateMs() {
    if (ttl != null) {
      return time.milliseconds() + ttl.toMillis();
    } else {
      return Long.MAX_VALUE;
    }
  }

}
