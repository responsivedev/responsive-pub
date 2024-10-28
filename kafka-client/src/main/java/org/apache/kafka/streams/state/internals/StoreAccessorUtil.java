package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateSerdes;

public class StoreAccessorUtil {

  @SuppressWarnings("unchecked")
  public static <K, V> StateSerdes<K, V> extractKeyValueStoreSerdes(
      final StateStore store
  ) {
    if (store instanceof MeteredKeyValueStore) {
      return ((MeteredKeyValueStore<K, V>) store).serdes;
    } else {
      throw new IllegalStateException("Attempted to extract serdes from store of type "
                                          + store.getClass().getName());
    }
  }
}
