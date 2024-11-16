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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class UniqueKeyBuffer<K extends Comparable<K>> implements SizeTrackingBuffer<K> {

  private final NavigableMap<K, Result<K>> buffer;
  private final KeySpec<K> extractor;
  private long bytes = 0L;

  public UniqueKeyBuffer(final KeySpec<K> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    this.buffer = new ConcurrentSkipListMap<>();
  }

  @Override
  public long sizeInBytes() {
    return bytes;
  }

  @Override
  public int sizeInRecords() {
    return buffer.size();
  }

  @Override
  public void put(final K key, final Result<K> value) {
    bytes += value.size(extractor);
    final Result<K> old = buffer.put(key, value);

    if (old != null) {
      bytes -= old.size(extractor);
    }
  }

  @Override
  public void clear() {
    bytes = 0;
    buffer.clear();
  }

  @Override
  public Result<K> get(final K key) {
    return buffer.get(key);
  }

  @Override
  public KeyValueIterator<K, Result<K>> range(final K from, final K to) {
    return Iterators.kv(
        buffer.subMap(from, true, to, true).entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> reverseRange(final K from, final K to) {
    return Iterators.kv(
        buffer.subMap(from, true, to, true).descendingMap().entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> all() {
    return Iterators.kv(
        buffer.entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> reverseAll() {
    return Iterators.kv(
        buffer.descendingMap().entrySet().iterator(),
        e -> new KeyValue<>(e.getKey(), e.getValue())
    );
  }

  @Override
  public Collection<Result<K>> values() {
    return buffer.values();
  }

}
