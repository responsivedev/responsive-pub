package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;

public class StoreAccessorUtil {

  @SuppressWarnings("rawtypes")
  public static StateSerdes<?, ?> extractKeyValueStoreSerdes(
      final StateStore store
  ) {
    if (store instanceof MeteredKeyValueStore) {
      return ((MeteredKeyValueStore) store).serdes;
    } else {
      throw new IllegalStateException("Attempted to extract serdes from store of type "
                                          + store.getClass().getName());
    }
  }
}
