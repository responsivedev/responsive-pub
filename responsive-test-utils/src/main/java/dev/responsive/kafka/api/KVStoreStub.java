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
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class KVStoreStub {

  private final Map<Bytes, byte[]> records = new HashMap<>();
  private Duration ttl;

  public KVStoreStub(final Duration ttl) {
    this.ttl = ttl;
    if (ttl != null) {
      throw new UnsupportedOperationException("ttl not yet implemented for TTD");
    }
  }

  public long count() {
    return records.size();
  }

  public void put(final Bytes key, final byte[] value) {
    records.put(key, value);
  }

  public void delete(final Bytes key) {
    records.remove(key);
  }

  public byte[] get(final Bytes key) {
    return records.get(key);
  }

  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  public KeyValueIterator<Bytes, byte[]> all() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }
}
