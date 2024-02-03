package org.apache.kafka.streams.state.internals;

import dev.responsive.kafka.api.async.AsyncKeyValueStore;
import org.apache.kafka.common.utils.Bytes;

public interface AsyncStoreSupplier<T extends AsyncKeyValueStore<Bytes, byte[]>> {
  String name();

  T getAsyncStore();
}
