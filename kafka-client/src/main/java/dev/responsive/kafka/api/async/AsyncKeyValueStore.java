package dev.responsive.kafka.api.async;

import java.util.function.Supplier;

public interface AsyncKeyValueStore<K, V> {
  ResponsiveFuture<Supplier<V>> putIfAbsentAsync(K key, V value);

  ResponsiveFuture<V> getAsync(K key);
}
