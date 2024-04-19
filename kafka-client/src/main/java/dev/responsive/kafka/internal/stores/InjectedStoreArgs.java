package dev.responsive.kafka.internal.stores;

import java.util.Optional;
import java.util.function.Supplier;

public class InjectedStoreArgs {

  private Supplier<Long> recordTimestampClock;

  public Optional<Supplier<Long>> recordTimestampClock() {
    return Optional.ofNullable(recordTimestampClock);
  }

  public void injectRecordTimestampClock(final Supplier<Long> recordTimestampClock) {
    this.recordTimestampClock = recordTimestampClock;
  }
}
