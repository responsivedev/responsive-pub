package dev.responsive.kafka.api.async.internals.contexts;

import dev.responsive.kafka.api.async.internals.events.DelayedWrite;

public interface DelayedAsyncStoreWriter {
  void acceptDelayedWriteToAsyncStore(DelayedWrite<?, ?> write);
}
