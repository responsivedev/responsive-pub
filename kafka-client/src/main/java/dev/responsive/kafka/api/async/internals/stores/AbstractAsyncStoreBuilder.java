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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

/**
 * A copy of the {@link org.apache.kafka.streams.state.internals.AbstractStoreBuilder} class with
 * a few additional methods related to async processing, such as de/registering flush listeners
 */
public abstract class AbstractAsyncStoreBuilder<T extends StateStore>
    implements StoreBuilder<T> {

  protected final String name;
  protected final Map<String, String> logConfig = new HashMap<>();

  private boolean cachingEnabled = false;
  private boolean loggingEnabled = true;

  // Since there is only one StoreBuilder instance for each store, it is used by all of the
  // StreamThreads in an app, and so we must account for which StreamThread is building
  // or accessing which stores
  private static final Map<String, Map<String, StreamThreadFlushListeners>> storeToThreadToListeners =
      new ConcurrentHashMap<>();

  public AbstractAsyncStoreBuilder(
      final String name
  ) {
    Objects.requireNonNull(name, "name cannot be null");
    this.name = name;
  }

  /**
   * Similar to the #maybeWrapCaching or #maybeWrapLogging methods in the
   * {@link org.apache.kafka.streams.state.internals.DelayedAsyncStoreBuilder},
   * this method adds an additional layer to the store
   * hierarchy by wrapping it in a {@link AsyncFlushingKeyValueStore}.
   * <p>
   * This specific method is for use with KV stores, whether plain or timestamped.
   */
  protected KeyValueStore<Bytes, byte[]> wrapAsyncFlushingKV(
      final KeyValueStore<Bytes, byte[]> inner
  ) {
    final StreamThreadFlushListeners threadFlushListeners =
        getOrCreateFlushListeners(inner.name(), Thread.currentThread().getName());

    return new AsyncFlushingKeyValueStore(inner, threadFlushListeners);
  }

  /**
   * Similar to the #maybeWrapCaching or #maybeWrapLogging methods in the
   * {@link org.apache.kafka.streams.state.internals.DelayedAsyncStoreBuilder},
   * this method adds an additional layer to the store
   * hierarchy by wrapping it in a {@link AsyncFlushingWindowStore}.
   * <p>
   * This specific method is for use with window stores, whether plain or timestamped.
   */
  protected WindowStore<Bytes, byte[]> wrapAsyncFlushingWindow(
      final WindowStore<Bytes, byte[]> inner
  ) {
    final StreamThreadFlushListeners threadFlushListeners =
        getOrCreateFlushListeners(inner.name(), Thread.currentThread().getName());

    return new AsyncFlushingWindowStore(inner, threadFlushListeners);
  }

  /**
   * Similar to the #maybeWrapCaching or #maybeWrapLogging methods in the
   * {@link org.apache.kafka.streams.state.internals.DelayedAsyncStoreBuilder},
   * this method adds an additional layer to the store
   * hierarchy by wrapping it in a {@link AsyncFlushingSessionStore}.
   * <p>
   * This specific method is for use with session stores, whether plain or timestamped.
   */
  protected SessionStore<Bytes, byte[]> wrapAsyncFlushingSession(
      final SessionStore<Bytes, byte[]> inner
  ) {
    final StreamThreadFlushListeners threadFlushListeners =
        getOrCreateFlushListeners(inner.name(), Thread.currentThread().getName());

    return new AsyncFlushingSessionStore(inner, threadFlushListeners);
  }

  /**
   * Register a flush listener and the partition it's associated with for the
   * given StreamThread.
   * <p>
   * Invoked from the async processor's #init which is called after the store has already
   * been built and initialized, so we expect this thread to already have registered a flush
   * listener for the store
   */
  public void registerFlushListenerWithAsyncStore(
      final String storeName,
      final String streamThreadName,
      final int partition,
      final AsyncFlushListener processorFlushListener
  ) {
    final var storeListeners = storeToThreadToListeners.get(storeName);
    if (storeListeners == null) {
      throw new IllegalStateException("Unable to locate flush listener metadata "
                                          + "for the current store: " + storeName);
    }

    final StreamThreadFlushListeners threadListeners =
        storeListeners.get(streamThreadName);
    if (threadListeners == null) {
      throw new IllegalStateException("Unable to locate flush listener metadata "
                                          + "for the current StreamThread: " + streamThreadName);
    }
    threadListeners.registerListenerForPartition(partition, processorFlushListener);
  }

  /**
   * Return the {@link StreamThreadFlushListeners} for this StreamThread,
   * creating/registering a new one if this is the first time we're seeing
   * the current StreamThread.
   * This should be a no-op if the builder has already registered this thread.
   */
  protected StreamThreadFlushListeners getOrCreateFlushListeners(
      final String storeName,
      final String streamThreadName
  ) {
    return storeToThreadToListeners
        .computeIfAbsent(storeName,
                         s -> new HashMap<>())
        .computeIfAbsent(streamThreadName,
                         k -> new StreamThreadFlushListeners(streamThreadName, name)
    );
  }

  @Override
  public StoreBuilder<T> withCachingEnabled() {
    cachingEnabled = true;
    return this;
  }

  @Override
  public StoreBuilder<T> withCachingDisabled() {
    cachingEnabled = false;
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
    Objects.requireNonNull(config, "config can't be null");
    loggingEnabled = true;
    logConfig.putAll(config);
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingDisabled() {
    loggingEnabled = false;
    logConfig.clear();
    return this;
  }

  @Override
  public Map<String, String> logConfig() {
    return logConfig;
  }

  @Override
  public boolean loggingEnabled() {
    return loggingEnabled;
  }

  // @Override annotation is not missing -- unlike loggingEnabled, there is no cachingEnabled API
  public boolean cachingEnabled() {
    return cachingEnabled;
  }

  @Override
  public String name() {
    return name;
  }
}
