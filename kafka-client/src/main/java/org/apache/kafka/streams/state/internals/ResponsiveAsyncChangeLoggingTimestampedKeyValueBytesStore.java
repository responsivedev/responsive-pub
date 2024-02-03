package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.ResponsiveFuture;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveAsyncChangeLoggingTimestampedKeyValueBytesStore<K, V>
    extends ChangeLoggingTimestampedKeyValueBytesStore
    implements AsyncKeyValueStore<K, V> {
  private final AsyncKeyValueStore<K, V> innerAsync;

  @SuppressWarnings("unchecked")
  ResponsiveAsyncChangeLoggingTimestampedKeyValueBytesStore(
      final AsyncKeyValueStore<K, V> inner) {
    super((KeyValueStore<Bytes, byte[]>) inner);
    this.innerAsync = inner;
  }

  @Override
  public ResponsiveFuture<Supplier<V>> putIfAbsentAsync(final K key, final V value) {
    return innerAsync.putIfAbsentAsync(key, value);
  }

  @Override
  public ResponsiveFuture<V> getAsync(final K key) {
    return innerAsync.getAsync(key);
  }
}
