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

package dev.responsive.internal.utils;

import dev.responsive.internal.db.KeySpec;

public class Result<K> {

  public final K key;
  public final byte[] value;
  public final boolean isTombstone;
  public final long timestamp;

  public static <K> Result<K> value(final K key, final byte[] value, long timestamp) {
    return new Result<>(key, value, false, timestamp);
  }

  public static <K> Result<K> tombstone(final K key, long timestamp) {
    return new Result<>(key, null, true, timestamp);
  }

  private Result(final K key, final byte[] value, final boolean isTombstone, long timestamp) {
    this.key = key;
    this.value = value;
    this.isTombstone = isTombstone;
    this.timestamp = timestamp;
  }

  public int size(final KeySpec<K> extractor) {
    return extractor.bytes(key).get().length
        + (isTombstone ? 0 : value.length)
        + Long.BYTES; // timestamp size
  }
}