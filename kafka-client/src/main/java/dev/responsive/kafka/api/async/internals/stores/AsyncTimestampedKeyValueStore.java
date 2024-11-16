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
