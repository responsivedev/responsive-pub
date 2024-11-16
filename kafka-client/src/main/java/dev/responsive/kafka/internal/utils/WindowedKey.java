/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.utils;

import java.util.Objects;
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowedKey that = (WindowedKey) o;
    return windowStartMs == that.windowStartMs
        && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, windowStartMs);
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
