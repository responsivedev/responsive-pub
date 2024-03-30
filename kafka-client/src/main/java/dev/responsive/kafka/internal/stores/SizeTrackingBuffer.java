/*
 *  Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SizeTrackingBuffer<K extends Comparable<K>> {
  private final NavigableMap<K, Result<K>> buffer;
  private final NavigableMap<K, Result<K>> reader;
  private final KeySpec<K> extractor;
  private long bytes = 0;

  public SizeTrackingBuffer(final KeySpec<K> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    buffer = new ConcurrentSkipListMap<>();
    reader = Collections.unmodifiableNavigableMap(buffer);
  }

  public long getBytes() {
    return bytes;
  }

  public void put(final K key, final Result<K> value) {
    bytes += value.size(extractor);
    final Result<K> old = buffer.put(key, value);
    if (old != null) {
      bytes -= old.size(extractor);
    }
  }

  public void clear() {
    bytes = 0;
    buffer.clear();
  }

  public NavigableMap<K, Result<K>> getReader() {
    return reader;
  }
}