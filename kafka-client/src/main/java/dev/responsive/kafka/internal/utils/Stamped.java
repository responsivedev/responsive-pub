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

package dev.responsive.kafka.internal.utils;

import org.apache.kafka.common.utils.Bytes;

public class Stamped implements Comparable<Stamped> {

  public final Bytes key;
  public final long timestamp;

  public Stamped(final Bytes key, final long timestamp) {
    this.key = key;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "Stamped{"
        + "key=" + key
        + ", windowStart=" + timestamp
        + '}';
  }

  @Override
  public int compareTo(final Stamped o) {
    final int compareKeys = this.key.compareTo(o.key);
    if (compareKeys != 0) {
      return compareKeys;
    } else {
      return Long.compare(this.timestamp, o.timestamp);
    }
  }
}
