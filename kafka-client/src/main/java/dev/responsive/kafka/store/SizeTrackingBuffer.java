package dev.responsive.kafka.store;

import dev.responsive.db.KeySpec;
import dev.responsive.model.Result;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class SizeTrackingBuffer<K> {
  private final NavigableMap<K, Result<K>> buffer;
  private final NavigableMap<K, Result<K>> reader;
  private final KeySpec<K> extractor;
  private long bytes = 0;

  public SizeTrackingBuffer(final KeySpec<K> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    buffer = new TreeMap<>(extractor);
    reader = Collections.unmodifiableNavigableMap(buffer);
  }

  public long getBytes() {
    return bytes;
  }

  public void put(final K key, final Result<K> value) {
    bytes += sizeOf(key, value);
    final Result<K> old = buffer.put(key, value);
    if (old != null) {
      bytes -= sizeOf(key, old);
    }
  }

  public void clear() {
    bytes = 0;
    buffer.clear();
  }

  public NavigableMap<K, Result<K>> getReader() {
    return reader;
  }

  private long sizeOf(final K key, final Result<K> value) {
    return extractor.bytes(key).get().length + (value.isTombstone ? 0 : value.value.length);
  }
}