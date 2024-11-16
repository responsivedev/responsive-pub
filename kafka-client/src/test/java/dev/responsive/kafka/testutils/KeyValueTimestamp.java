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

package dev.responsive.kafka.testutils;

import java.util.Objects;

public class KeyValueTimestamp<K, V> {
  private final K key;
  private final V value;
  private final long timestamp;

  public KeyValueTimestamp(final K key, final V value, final long timestamp) {
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  public long timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "KeyValueTimestamp{key=" + key + ", value=" + value + ", timestamp=" + timestamp + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyValueTimestamp<?, ?> that = (KeyValueTimestamp<?, ?>) o;
    return timestamp == that.timestamp
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, timestamp);
  }
}
