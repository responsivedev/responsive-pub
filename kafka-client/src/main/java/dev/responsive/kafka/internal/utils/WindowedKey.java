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

public class WindowedKey implements Comparable<WindowedKey> {

  public final Bytes key;
  public final long windowStartMs;

  public WindowedKey(final Bytes key, final long windowStartMs) {
    this.key = key;
    this.windowStartMs = windowStartMs;
  }

  public WindowedKey(final byte[] key, final long windowStartMs) {
    this.key = Bytes.wrap(key);
    this.windowStartMs = windowStartMs;
  }

  @Override
  public String toString() {
    return "WindowedKey{"
        + "key=" + key
        + ", windowStartMs=" + windowStartMs
        + '}';
  }

  @Override
  public int compareTo(final WindowedKey o) {
    final int compareKeys = this.key.compareTo(o.key);
    if (compareKeys != 0) {
      return compareKeys;
    } else {
      return Long.compare(this.windowStartMs, o.windowStartMs);
    }
  }

}
