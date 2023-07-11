package dev.responsive.kafka.store;

import dev.responsive.model.Result;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class SizeTrackingBuffer<K> {
  private final NavigableMap<K, Result<K>> buffer;
  private final NavigableMap<K, Result<K>> reader;
  private final BufferPlugin<K> plugin;
  private long bytes = 0;

  public SizeTrackingBuffer(final BufferPlugin<K> plugin) {
    this.plugin = Objects.requireNonNull(plugin);
    buffer = new TreeMap<>(plugin);
    reader = Collections.unmodifiableNavigableMap(buffer);
  }

  public long getBytes() {
    return bytes;
  }

  public void put(final K key, final Result<K> value) {
    bytes += plugin.bytes(key).get().length + (value.isTombstone ? 0 : value.value.length);
    buffer.put(key, value);
  }

  public void clear() {
    bytes = 0;
    buffer.clear();
  }

  public NavigableMap<K, Result<K>> reader() {
    return reader;
  }
}
