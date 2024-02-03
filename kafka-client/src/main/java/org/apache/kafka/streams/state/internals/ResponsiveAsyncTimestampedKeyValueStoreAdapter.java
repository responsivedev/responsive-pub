package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;

import dev.responsive.kafka.api.async.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.ResponsiveFuture;
import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

public class ResponsiveAsyncTimestampedKeyValueStoreAdapter
    extends KeyValueToTimestampedKeyValueByteStoreAdapter
    implements AsyncKeyValueStore<Bytes, byte[]> {
  private final ResponsiveKeyValueStore responsiveStore;

  ResponsiveAsyncTimestampedKeyValueStoreAdapter(final ResponsiveKeyValueStore store) {
    super(store);
    this.responsiveStore = Objects.requireNonNull(store);
  }

  @Override
  public ResponsiveFuture<Supplier<byte[]>> putIfAbsentAsync(final Bytes key, final byte[] value) {
    return responsiveStore.putIfAbsentAsync(key, rawValue(value))
        .thenApply(supplier -> () -> convertToTimestampedFormat(supplier.get()));
  }

  @Override
  public ResponsiveFuture<byte[]> getAsync(final Bytes key) {
    return responsiveStore.getAsync(key)
        .thenApply(TimestampedBytesStore::convertToTimestampedFormat);
  }


}
