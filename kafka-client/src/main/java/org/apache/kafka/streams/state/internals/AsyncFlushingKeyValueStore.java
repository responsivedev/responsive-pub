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

package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.kafka.api.async.internals.stores.AsyncProcessorFlushers.AsyncFlushListenerSupplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueStore;
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
public class AsyncFlushingKeyValueStore implements AsyncFlushingStore
    extends CachingKeyValueStore {

  private final Logger log;

  // These two values, together with the partition (which we can't get until #init
  // as discussed in the comment below), allow us to uniquely identify the specific
  // AsyncProcessor instance this store belongs to, and ultimately obtain the
  // correct flush listener for that processor
  private final String streamThreadName;
  private final String storeName;
  private final AsyncFlushListenerSupplier listenerSupplier;

  // Unfortunately the store builders are all constructed from the main application,
  // not the StreamThread we need to issue the flush call on, and these builders
  // will build the actual StateStore layers (including this) as well as the
  // AsyncProcessor instance when the task is first created, at which point we
  // don't have access to all the information we need to actually link them
  // together (specifically, the partition). We have to wait until #init is
  // called on both the AsyncProcessor and the StateStore so we can access the
  // partition info, and then together with the StreamThread name and store name,
  // can finally determine which AsyncProcessor we should hook up to and flush
  // when a commit occurs.

  // However, once initialized, this field is effectively final and should never
  // change
  private Runnable flushAsyncBuffers;

  public AsyncFlushingKeyValueStore(
      final KeyValueStore<Bytes, byte[]> underlying,
      final boolean timestampedSchema
  ) {
    super(underlying, timestampedSchema);
    this.storeName = underlying.name();
    this.streamThreadName = Thread.currentThread().getName();
    this.log = new LogContext(
        String.format("stream-thread [%s] %s: ", streamThreadName, storeName)
    ).logger(AsyncFlushingKeyValueStore.class);
  }

  /**
   * Used to initialize the callback that will be invoked on each commit, prior to flushing
   * the cache itself. This should result in the StreamThread blocking until all pending
   * async events corresponding to the offsets contained in this commit have been fully
   * processed from start to finish (including all "side effects" like writes and forwards)
   */
  public void setAsyncBufferFlusher(final Runnable flushAsyncBuffers) {
    if (this.flushAsyncBuffers != null) {
      throw new IllegalStateException("Attempted to set the async buffer flush callback but it"
                                          + "was already initialized");
    }
    this.flushAsyncBuffers = flushAsyncBuffers;
  }

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

    initInternal(asInternalProcessorContext(context));
    super.init(context, root);
    // save the stream thread as we only ever want to trigger a flush
    // when the stream thread is the current thread.
    streamThread = Thread.currentThread();
  }

  @Override
  public void flushCache() {
    // We wait on/clear out the async processor buffers first so that any pending async events
    // that write to the state store are guaranteed to be inserted in the cache before we
    // proceed with flushing the cache. This is the reason we hook into the commit to block
    // on pending async results via this #flushCache API, and not, for example, the consumer's
    // commit or producer's commitTxn -- because #flushCache is the first call in a commit, and
    // if we waited until #commit/#commitTxn we would have to flush the cache a second time in
    // case any pending async events issued new writes to the state store/cache
    flushAsyncBuffers.run();

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
}
