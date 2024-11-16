/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;

public abstract class KVFlushManager implements FlushManager<Bytes, Integer> {

  @Override
  public void writeAdded(final Bytes key) {
    // nothing to do
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    // the pre-flush hook is only needed for window stores, so just return success
    return RemoteWriteResult.success(null);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
    return updateOffset(consumedOffset);
  }

  public abstract RemoteWriteResult<Integer> updateOffset(
      final long consumedOffset
  );
}
