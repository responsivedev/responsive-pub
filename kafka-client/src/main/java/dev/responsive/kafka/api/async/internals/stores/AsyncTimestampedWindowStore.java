package dev.responsive.kafka.api.async.internals.stores;

import dev.responsive.kafka.api.async.internals.contexts.DelayedAsyncStoreWriter;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

public class AsyncTimestampedWindowStore<KS, VS>
    extends AsyncWindowStore<KS, ValueAndTimestamp<VS>>
    implements TimestampedWindowStore<KS, VS> {

  public AsyncTimestampedWindowStore(
      final String name,
      final int partition,
      final WindowStore<?, ?> userDelegate,
      final DelayedAsyncStoreWriter delayedWriter
  ) {
    super(name, partition, userDelegate, delayedWriter);
  }

}
