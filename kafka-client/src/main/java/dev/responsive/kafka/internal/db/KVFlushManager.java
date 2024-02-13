/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
