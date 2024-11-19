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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AsyncKeyValueStoreBuilder;

/**
 * A copy of the {@link org.apache.kafka.streams.state.internals.AbstractStoreBuilder} class with
 * a few additional methods related to async processing, such as de/registering flush listeners
 */
public abstract class AbstractAsyncStoreBuilder<K, V, T extends StateStore>
    implements StoreBuilder<T> {

  protected final String name;
  protected Serde<K> keySerde;
  protected Serde<V> valueSerde;
  protected Time time;
  protected final Map<String, String> logConfig = new HashMap<>();

  private boolean cachingEnabled = false;
  private boolean loggingEnabled = true;

  // Since there is only one StoreBuilder instance for each store, it is used by all of the
  // StreamThreads in an app, and so we must account for which StreamThread is building
  // or accessing which stores
  private final Map<String, StreamThreadFlushListeners> streamThreadToFlushListeners =
      new ConcurrentHashMap<>();

  public AbstractAsyncStoreBuilder(
      final String name
  ) {
    Objects.requireNonNull(name, "name cannot be null");
    this.name = name;
  }

  public void initialize(final Serde<K> keySerde,
                         final Serde<V> valueSerde,
                         final Time time) {
    Objects.requireNonNull(time, "time cannot be null");

    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.time = time;
  }

  /**
   * Similar to the #maybeWrapCaching or #maybeWrapLogging methods in the StoreBuilder classes
   * (eg {@link AsyncKeyValueStoreBuilder}, this method adds an additional layer to the store
   * hierarchy by wrapping it in a {@link AsyncFlushingKeyValueStore}.
   * <p>
   * This specific method is for use with KV stores, whether plain or timestamped.
   * TODO: add equivalent for window/session stores
   */
  protected KeyValueStore<Bytes, byte[]> wrapAsyncFlushingKV(
      final KeyValueStore<Bytes, byte[]> inner
  ) {
    final StreamThreadFlushListeners threadFlushListeners =
        getOrCreateFlushListeners(Thread.currentThread().getName());

    return new AsyncFlushingKeyValueStore(inner, threadFlushListeners);
  }

  /**
   * Register a flush listener and the partition it's associated with for the
   * given StreamThread.
   */
  public void registerFlushListenerWithAsyncStore(
      final String streamThreadName,
      final int partition,
      final AsyncFlushListener processorFlushListener
  ) {
    final StreamThreadFlushListeners threadListeners =
        streamThreadToFlushListeners.get(streamThreadName);
    if (threadListeners == null) {
      throw new IllegalStateException("Unable to locate flush listener metadata "
                                          + "for the current StreamThread");
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
      final String streamThreadName
  ) {
    return streamThreadToFlushListeners.computeIfAbsent(
        streamThreadName,
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
