package dev.responsive.kafka.api.async.internals.stores;

import dev.responsive.kafka.api.async.internals.events.DelayedWrite;

public interface AsyncStateStore<KS, VS> {

  void executeDelayedWrite(final DelayedWrite<KS, VS> delayedWrite);
}
