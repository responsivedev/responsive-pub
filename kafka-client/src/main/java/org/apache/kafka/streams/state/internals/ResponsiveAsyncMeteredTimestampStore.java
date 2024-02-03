package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.AsyncTimestampedKeyValueStore;
import dev.responsive.kafka.api.async.ResponsiveFuture;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class ResponsiveAsyncMeteredTimestampStore<K, V>
    extends MeteredTimestampedKeyValueStore<K, V>
    implements AsyncTimestampedKeyValueStore<K, V> {
  private final AsyncKeyValueStore<Bytes, byte[]> asyncInner;

  @SuppressWarnings("unchecked")
  ResponsiveAsyncMeteredTimestampStore(
      final AsyncKeyValueStore<Bytes, byte[]> inner, final String metricScope,
      final Time time,
      final Serde<K> keySerde,
      final Serde<ValueAndTimestamp<V>> valueSerde) {
    super((KeyValueStore<Bytes, byte[]>) inner, metricScope, time, keySerde, valueSerde);
    this.asyncInner = Objects.requireNonNull(inner);
  }


  @Override
  public ResponsiveFuture<Supplier<ValueAndTimestamp<V>>> putIfAbsentAsync(
      final K key, final ValueAndTimestamp<V> value
  ) {
    return asyncInner.putIfAbsentAsync(keyBytes(key), serdes.rawValue(value))
        .thenApply(s -> () -> outerValue(s.get()));
  }

  @Override
  public ResponsiveFuture<ValueAndTimestamp<V>> getAsync(final K key) {
    return asyncInner.getAsync(keyBytes(key))
        .thenApply(this::outerValue);
  }
}
