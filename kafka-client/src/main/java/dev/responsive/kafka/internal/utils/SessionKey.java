/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
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
    return Arrays.equals(this.key.get(), sessionKey.key.get())
        && this.sessionEndMs == sessionKey.sessionEndMs
        && this.sessionStartMs == sessionKey.sessionStartMs;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(this.sessionEndMs, this.sessionStartMs);
    result = 31 * result + Arrays.hashCode(this.key.get());
    return result;
  }
}
