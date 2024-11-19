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

import dev.responsive.kafka.api.async.internals.stores.StreamThreadFlushListeners.AsyncFlushListener;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.internals.CacheFlushListener;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;

public abstract class AbstractAsyncFlushingStore<S extends StateStore>
    extends WrappedStateStore<S, byte[], byte[]>
    implements CachedStateStore<byte[], byte[]> {

  private final Logger log;

  private final StreamThreadFlushListeners flushListeners;

  // Effectively final but can't be initialized until the store's #init
  private int partition;

  // Effectively final but can't be initialized until the corresponding processor's #init
  private AsyncFlushListener flushAsyncProcessor;

  public AbstractAsyncFlushingStore(
      final S inner,
      final StreamThreadFlushListeners flushListeners
  ) {
    super(inner);
    this.flushListeners = flushListeners;
    this.log = new LogContext(
        String.format("stream-thread [%s] %s: ",
                      flushListeners.streamThreadName(),
                      inner.name())
    ).logger(AbstractAsyncFlushingStore.class);
  }

  @Override
  public void init(final StateStoreContext context,
                   final StateStore root) {
    this.partition = context.taskId().partition();

    flushListeners.registerStoreConnectorForPartition(
        partition,
        flushListener -> flushAsyncProcessor = flushListener
    );

    try {
      super.init(context, root);
    } catch (final RuntimeException e) {
      log.error("failed to initialize the wrapped store. Deregistering the store connector as "
                    + "its likely that this store was not registered with streams and close will "
                    + "not be called");
      flushListeners.unregisterListenerForPartition(partition);
      throw e;
    }
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
   * Used by Streams to clear the cache (without flushing) when a task is transitioning
   * from active to standby and the state stores are being recycled. Standby tasks
   * have no caching layer, so Streams simply clears the cache here in case the
   * task is re-recycled back into an active task and the caching layer is revived.
   */
  @Override
  public void clearCache() {
    // this is technically a Responsive-specific constraint, and should be relaxed if we open
    // up the async framework to general use cases
    throw new IllegalStateException(
        "Attempted to clear cache of async store, this implies the task is "
            + "transitioning to standby which should not happen");
  }

  @Override
  public void close() {
    flushListeners.unregisterListenerForPartition(partition);
    super.close();
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

}
