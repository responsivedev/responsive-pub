package dev.responsive.kafka.api.async.internals.stores;

import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class AsyncTimestampedKeyValueStore<KS, VS>
    extends AsyncKeyValueStore<KS, ValueAndTimestamp<VS>>
    implements TimestampedKeyValueStore<KS, VS>
{
  public AsyncTimestampedKeyValueStore(
      final String name,
      final int partition,
      final KeyValueStore<?, ?> userDelegate
  ) {
    super(name, partition, userDelegate);
  }
}
