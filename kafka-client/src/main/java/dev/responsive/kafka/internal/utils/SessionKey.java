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

public class SessionKey implements Comparable<SessionKey> {

  public final Bytes key;
  public final long sessionStartMs;
  public final long sessionEndMs;

  public SessionKey(final Bytes key, final long sessionStartMs, final long sessionEndMs) {
    this.key = key;
    this.sessionStartMs = sessionStartMs;
    this.sessionEndMs = sessionEndMs;
  }

  public SessionKey(final byte[] key, final long sessionStartMs, final long sessionEndMs) {
    this(Bytes.wrap(key), sessionStartMs, sessionEndMs);
  }

  @Override
  public String toString() {
    return "SessionKey{"
        + "key=" + this.key
        + ", sessionStartMs=" + this.sessionStartMs
        + ", sessionEndMs=" + this.sessionEndMs
        + '}';
  }

  @Override
  public int compareTo(final SessionKey o) {
    final int compareKeys = this.key.compareTo(o.key);
    if (compareKeys != 0) {
      return compareKeys;
    }

    final int compareStarts = Long.compare(this.sessionStartMs, o.sessionStartMs);
    if (compareStarts != 0) {
      return compareStarts;
    }

    return Long.compare(this.sessionEndMs, o.sessionEndMs);
  }
}
