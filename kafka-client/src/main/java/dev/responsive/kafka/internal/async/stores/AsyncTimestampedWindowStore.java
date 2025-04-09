/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.async.stores;

import dev.responsive.kafka.internal.async.contexts.DelayedAsyncStoreWriter;
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