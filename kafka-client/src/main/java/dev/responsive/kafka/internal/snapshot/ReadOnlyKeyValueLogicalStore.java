package dev.responsive.kafka.internal.snapshot;

import org.apache.kafka.streams.state.KeyValueIterator;

public interface ReadOnlyKeyValueLogicalStore<K, V> extends AutoCloseable {
  V get(K key);

  KeyValueIterator<K, V> range(K from, K to);

  KeyValueIterator<K, V> all();

  @Override
  void close();
}
