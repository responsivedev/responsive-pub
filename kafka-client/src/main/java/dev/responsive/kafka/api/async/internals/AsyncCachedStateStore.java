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

package dev.responsive.kafka.api.async.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;

public abstract class AsyncCachedStateStore implements CachedStateStore<byte[], byte[]> {

  private final CachedStateStore<byte[], byte[]> innerStore;

  public AsyncCachedStateStore(final CachedStateStore<byte[], byte[]> innerStore) {
    this.innerStore = innerStore;
  }

  @Override
  public boolean setFlushListener(
      final CacheFlushListener<byte[], byte[]> listener,
      final boolean sendOldValues
  ) {
    final boolean cachingEnabled = innerStore.setFlushListener(listener, sendOldValues);

    // TODO -- hook evicted records up into async pipeline?
    throw new IllegalStateException("TODO");
    // return cachingEnabled;
  }

  @Override
  public void flushCache() {
    // TODO -- this is where we begin the async commit process to clear out
    //  pending processors
    throw new IllegalStateException("TODO");
  }

  /**
   * Clear the cache; this is used when a task/store is changed in such a way
   * that the cache is invalidated/out of date, such as when an active task
   * is recycled into a standby (since only active task stores get a Streams cache)
   * <p>
   * Note this call should not try to flush the cache, and it's assumed that the cache
   * has already been flushed and thus does not contain any dirty and
   * unwritten/unprocessed pending entries
   * <p>
   * Note to self: the Streams javadocs claim this method is a "hack" and will be removed
   * when we decouple caching from emitting in the future. We should keep an eye on this
   * but that entails a huge refactor that is unlikely to happen any time soon. Perhaps
   * more relevantly, a Responsive store should never be transitioned from active to standby,
   * so this should really never be called -- at least not when using Responsive.
   */
  @Override
  public void clearCache() {
    innerStore.clearCache();
  }

  public static class AsyncCachingKeyValueStore
      extends AsyncCachedStateStore, CachingKeyValueStore {

    public AsyncCachingKeyValueStore(final CachingKeyValueStore innerStore) {
      super(innerStore);
    }
  }
}
