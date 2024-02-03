package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.AsyncKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class AsyncTimestampedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> {

  private final AsyncStoreSupplier<ResponsiveKeyValueStore> storeSupplier;

  public AsyncTimestampedKeyValueStoreBuilder(
      final AsyncStoreSupplier<ResponsiveKeyValueStore> storeSupplier,
      final Serde<K> keySerde,
      final Serde<V> valueSerde,
      final Time time) {
    super(
        storeSupplier.name(),
        keySerde,
        valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
        time);
    Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
    this.storeSupplier = storeSupplier;
  }

  @Override
  public TimestampedKeyValueStore<K, V> build() {
    ResponsiveKeyValueStore store = storeSupplier.getAsyncStore();
    AsyncKeyValueStore<Bytes, byte[]> wrapped;
    wrapped = new ResponsiveAsyncTimestampedKeyValueStoreAdapter(store);
    return new ResponsiveAsyncMeteredTimestampStore<>(
        maybeWrapLogging(wrapped),
        "test.scope",
        time,
        keySerde,
        valueSerde
    );
  }

  private AsyncKeyValueStore<Bytes, byte[]> maybeWrapLogging(final AsyncKeyValueStore<Bytes, byte[]> inner) {
    if (!enableLogging) {
      return inner;
    }
    return new ResponsiveAsyncChangeLoggingTimestampedKeyValueBytesStore<>(inner);
  }
}
