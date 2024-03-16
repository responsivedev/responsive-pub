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
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;

/**
 * Simple wrapper class around Kafka Streams' {@link CachingKeyValueStore} class that
 * we use to hook into the {@link CachedStateStore#flushCache()} API and ensure that
 * all async processing is completed before Streams can proceed with a commit.
 * <p>
 * Note: this relies on non-public APIs in Streams and is therefore not protected
 * by the usual compatibility guarantees provided by Apache Kafka. It may break
 * upon upgrade. (This is also why it's placed in the o.a.k.streams.state.internals
 * package -- so we can call the package-private constructor of the super class)
 */
public class AsyncFlushingKeyValueStore
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, byte[], byte[]>
    implements KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> {

  private final Logger log;

  private final StreamThreadFlushListeners flushListeners;

  private AsyncFlushListener flushAsyncProcessor;

  public AsyncFlushingKeyValueStore(
      final KeyValueStore<Bytes, byte[]> inner,
      final StreamThreadFlushListeners flushListeners
  ) {
    super(inner);
    this.flushListeners = flushListeners;
    this.log = new LogContext(
        String.format("stream-thread [%s] %s: ",
                      flushListeners.streamThreadName(), inner.name())
    ).logger(AsyncFlushingKeyValueStore.class);
  }

  /**
   * Invoked when the state store is initialized and we can finally access the partition
   * of the task it belongs to, which is the final puzzle piece needed to uniquely
   * identify the AsyncProcessor this store is connected to and initialize the correct
   * async flush listener for it.
   */
  private void initializeAsyncFlushListener(final int partition) {
    flushAsyncProcessor = flushListeners.retrieveListenerForPartition(partition);
  }

  /**
   * NOTE: this is NOT the same as the AsyncFlushListener, which is used to flush the entire
   * async processor when the cache is flushed as part of a Streams commit.
   * This API is used by Streams, internally, to register a listener for the records that
   * are evicted from the Streams cache and need to be forwarded downstream through the
   * topology. This method would be better named #setCacheEvictionListener since evictions
   * can happen when a new record is added that pushes the cache beyond its maximum size,
   * and not just when the cache is flushed. Unfortunately, it's a Streams API that we're
   * being forced to implement here, not something we can control.
   */
  @Override
  public boolean setFlushListener(
      final CacheFlushListener<byte[], byte[]> listener,
      final boolean sendOldValues
  ) {
    return super.setFlushListener(listener, sendOldValues);
  }

  @Override
  public void init(final StateStoreContext context,
                   final StateStore root) {
    initializeAsyncFlushListener(context.taskId().partition());

    super.init(context, root);
  }

  @Override
  public void flushCache() {
    if (flushAsyncProcessor != null) {
      // We wait on/clear out the async processor buffers first so that any pending async events
      // that write to the state store are guaranteed to be inserted in the cache before we
      // proceed with flushing the cache. This is the reason we hook into the commit to block
      // on pending async results via this #flushCache API, and not, for example, the consumer's
      // commit or producer's commitTxn -- because #flushCache is the first call in a commit, and
      // if we waited until #commit/#commitTxn we would have to flush the cache a second time in
      // case any pending async events issued new writes to the state store/cache
      flushAsyncProcessor.flushBuffers();
    } else {
      log.warn("A flush was triggered on the async state store but the flush listener was "
                   + "not yet initialized. This can happen when a task is closed before "
                   + "it can be initialized.");
    }

    super.flushCache();
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
    super.clearCache();
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    wrapped().put(key, value);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    return wrapped().putIfAbsent(key, value);
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    wrapped().putAll(entries);
  }

  @Override
  public byte[] delete(final Bytes key) {
    return wrapped().delete(key);
  }

  @Override
  public byte[] get(final Bytes key) {
    return wrapped().get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return wrapped().range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return wrapped().all();
  }

  @Override
  public long approximateNumEntries() {
    return wrapped().approximateNumEntries();
  }
}
