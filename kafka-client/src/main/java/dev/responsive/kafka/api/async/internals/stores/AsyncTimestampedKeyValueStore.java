package dev.responsive.kafka.api.async.internals.stores;

import dev.responsive.kafka.api.async.internals.contexts.DelayedAsyncStoreWriter;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class AsyncTimestampedKeyValueStore<KS, VS>
    extends AsyncKeyValueStore<KS, ValueAndTimestamp<VS>>
    implements TimestampedKeyValueStore<KS, VS> {

  public AsyncTimestampedKeyValueStore(
      final String name,
      final int partition,
      final KeyValueStore<?, ?> userDelegate,
      final DelayedAsyncStoreWriter delayedWriter
  ) {
    super(name, partition, userDelegate, delayedWriter);
  }

}
