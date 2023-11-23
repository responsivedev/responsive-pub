package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.utils.Result;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class SizeTrackingBuffer<K extends Comparable<K>> {
  private final NavigableMap<K, Result<K>> buffer;
  private final NavigableMap<K, Result<K>> reader;
  private final KeySpec<K> extractor;
  private long bytes = 0;

  public SizeTrackingBuffer(final KeySpec<K> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
    buffer = new TreeMap<>();
    reader = Collections.unmodifiableNavigableMap(buffer);
  }

  public long getBytes() {
    return bytes;
  }

  public void put(final K key, final Result<K> value) {
    bytes += value.size(extractor);
    final Result<K> old = buffer.put(key, value);
    if (old != null) {
      bytes -= old.size(extractor);
    }
  }

  public void clear() {
    bytes = 0;
    buffer.clear();
  }

  public NavigableMap<K, Result<K>> getReader() {
    return reader;
  }
}