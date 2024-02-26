/*
 * Copyright 2024 Responsive Computing, Inc.
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

import java.util.Arrays;
import java.util.Objects;
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
        + ", sessionEndMs=" + this.sessionEndMs
        + ", sessionStartMs=" + this.sessionStartMs
        + '}';
  }

  @Override
  public int compareTo(final SessionKey o) {
    final int compareKeys = this.key.compareTo(o.key);
    if (compareKeys != 0) {
      return compareKeys;
    }

    final int compareEnds = Long.compare(this.sessionEndMs, o.sessionEndMs);
    if (compareEnds != 0) {
      return compareEnds;
    }

    return Long.compare(this.sessionStartMs, o.sessionStartMs);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionKey sessionKey = (SessionKey) o;
    return Arrays.equals(this.key.get(), sessionKey.key.get()) &&
        this.sessionEndMs == sessionKey.sessionEndMs
        && this.sessionStartMs == sessionKey.sessionStartMs;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(this.sessionEndMs, this.sessionStartMs);
    result = 31 * result + Arrays.hashCode(this.key.get());
    return result;
  }
}
