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

package dev.responsive.kafka.api;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueIterator;

public class KVStoreStub {

  private final Map<Bytes, Record> records = new LinkedHashMap<>();

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
    records.put(key, new Record(value));
  }

  public void delete(final Bytes key) {
    records.remove(key);
  }

  public byte[] get(final Bytes key) {
    expireRecords();
    final Record record = records.get(key);
    if (record != null) {
      return record.value;
    } else {
      return null;
    }
  }

  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    expireRecords();
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<Bytes, byte[]> all() {
    expireRecords();
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  private void expireRecords() {
    final long now = time.milliseconds();
    final var iter = records.entrySet().iterator();
    while (iter.hasNext()) {
      final var entry = iter.next();
      if (entry.getValue().expirationDateMs < now) {
         iter.remove();
      } else {
        break;
      }
    }
  }

  private class Record {
    final byte[] value;
    final long expirationDateMs;

    public Record(final byte[] value) {
      this.value = value;
      if (ttl != null) {
        expirationDateMs = time.milliseconds() + ttl.toMillis();
      } else {
        expirationDateMs = Long.MAX_VALUE;
      }
    }
  }

}
