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

import dev.responsive.kafka.internal.db.KeySpec;

public class Result<K extends Comparable<K>> {

  public final K key;
  public final byte[] value;
  public final boolean isTombstone;
  public final long timestamp;

  public static <K extends Comparable<K>> Result<K> value(
      final K key,
      final byte[] value,
      final long timestamp
  ) {
    return new Result<>(key, value, false, timestamp);
  }

  public static <K extends Comparable<K>> Result<K> tombstone(
      final K key,
      final long timestamp
  ) {
    return new Result<>(key, null, true, timestamp);
  }

  private Result(final K key, final byte[] value, final boolean isTombstone, final long timestamp) {
    this.key = key;
    this.value = value;
    this.isTombstone = isTombstone;
    this.timestamp = timestamp;
  }

  public int size(final KeySpec<K> extractor) {
    return extractor.sizeInBytes(key)
        + (isTombstone ? 0 : value.length)
        + Long.BYTES; // timestamp size
  }
}