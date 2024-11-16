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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class DuplicateKeyBuffer<K extends Comparable<K>> implements SizeTrackingBuffer<K> {

  private final NavigableMap<K, List<Result<K>>> buffer;
  private final KeySpec<K> extractor;
  private long bytes = 0L;
  private int size = 0;

  public DuplicateKeyBuffer(final KeySpec<K> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    this.buffer = new ConcurrentSkipListMap<>();
  }

  @Override
  public long sizeInBytes() {
    return bytes;
  }

  @Override
  public int sizeInRecords() {
    return size;
  }

  @Override
  public void put(final K key, final Result<K> value) {
    // ignore tombstones since deletes are a no-op for duplicate stores
    if (!value.isTombstone) {
      bytes += value.size(extractor);
      ++size;

      buffer.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
  }

  @Override
  public void clear() {
    bytes = 0;
    size = 0;
    buffer.clear();
  }

  @Override
  public Result<K> get(final K key) {
    if (!buffer.containsKey(key)) {
      return null;
    } else {
      // point lookups are not defined for stores with duplicates, so just return the first element
      return buffer.get(key).get(0);
    }
  }

  @Override
  public KeyValueIterator<K, Result<K>> range(final K from, final K to) {
    return Iterators.kv(
        buffer
            .subMap(from, true, to, true)
            .entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(r -> new KeyValue<>(e.getKey(), r)))
            .iterator(),
        kv -> kv
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> reverseRange(final K from, final K to) {
    return Iterators.kv(
        buffer
            .subMap(from, true, to, true)
            .descendingMap()
            .entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(r -> new KeyValue<>(e.getKey(), r)))
            .iterator(),
        kv -> kv
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> all() {
    return Iterators.kv(
        buffer
            .entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(r -> new KeyValue<>(e.getKey(), r)))
            .iterator(),
        kv -> kv
    );
  }

  @Override
  public KeyValueIterator<K, Result<K>> reverseAll() {
    return Iterators.kv(
        buffer
            .descendingMap()
            .entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(r -> new KeyValue<>(e.getKey(), r)))
            .iterator(),
        kv -> kv
    );
  }

  @Override
  public Collection<Result<K>> values() {
    return buffer.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

}
