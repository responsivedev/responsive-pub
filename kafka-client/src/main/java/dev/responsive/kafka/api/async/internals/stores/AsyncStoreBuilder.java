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

package dev.responsive.kafka.api.async.internals.stores;

import dev.responsive.kafka.api.async.internals.stores.StreamThreadFlushListeners.AsyncFlushListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

public interface AsyncStoreBuilder<T extends StateStore> extends StoreBuilder<T> {

  /**
   * Register a flush listener and the partition it's associated with for the
   * given StreamThread.
   */
  void registerFlushListenerWithAsyncStore(
      final String streamThreadName,
      final int partition,
      final AsyncFlushListener processorFlushListener
  );

  /**
   * Unregister the flush listener for this partition from the given StreamThread,
   * if one exists and has not yet been initialized and removed.
   * This should always be called when a processor is closed to reset the flush
   * listeners and free up this partition in case the StreamThread re-initializes
   * or is re-assigned the same StreamTask/partition again later.
   * This should be a no-op if the flush listener was not yet registered, or if
   * it was registered and then removed already due to being initialized by the
   * corresponding state store. In theory, this method only performs any action
   * when a task happens to be closed while it's in the middle of initialization,
   * which should be rare although possible.
   */
  void unregisterFlushListenerForPartition(
      final String streamThreadName,
      final int partition
  );
}
