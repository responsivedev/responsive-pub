/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

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
