package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import org.apache.kafka.streams.state.TimestampedBytesStore;

public class ResponsiveTimestampedKeyValueStore
    extends ResponsiveKeyValueStore implements TimestampedBytesStore {


  public ResponsiveTimestampedKeyValueStore(
      final ResponsiveKeyValueParams params
  ) {
    super(params, true);
  }

  public ResponsiveTimestampedKeyValueStore(
      final ResponsiveKeyValueParams params,
      final KVOperationsProvider opsProvider
  ) {
    super(params, true, opsProvider);
  }
}
