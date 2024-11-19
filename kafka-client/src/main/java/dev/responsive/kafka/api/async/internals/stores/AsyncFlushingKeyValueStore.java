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
    extends AbstractAsyncFlushingStore<KeyValueStore<Bytes, byte[]>>
    implements KeyValueStore<Bytes, byte[]> {

  public AsyncFlushingKeyValueStore(
      final KeyValueStore<Bytes, byte[]> inner,
      final StreamThreadFlushListeners flushListeners
  ) {
    super(inner, flushListeners);
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
