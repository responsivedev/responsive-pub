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

package dev.responsive.kafka.internal.async.stores;

import dev.responsive.kafka.internal.async.contexts.DelayedAsyncStoreWriter;
import dev.responsive.kafka.internal.async.events.DelayedWrite;
import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;

/**
 * A wrapper around the actual state store that's used to intercept writes that occur
 * during processing by the AsyncThread, so they can be passed back to the StreamThread
 * and executed there in case of cache evictions that trigger downstream processing
 * All variants of #put (eg #putAll and #putIfAbsent) are translated into actual
 * #put calls, so that we only need to queue up one type of write
 * <p>
 * Threading notes:
 * -Methods should only be invoked from an AsyncThread
 * -One for each AsyncThread per physical state store instance
 *   (ie per state store per processor per partition per AsyncThread per StreamThread
 */
public class AsyncKeyValueStore<KS, VS>
    extends WrappedStateStore<KeyValueStore<?, ?>, KS, VS>
    implements KeyValueStore<KS, VS> {

  private final Logger log;

  private final KeyValueStore<KS, VS> userDelegate;
  private final DelayedAsyncStoreWriter delayedWriter;

  @SuppressWarnings("unchecked")
  public AsyncKeyValueStore(
      final String name,
      final int partition,
      final KeyValueStore<?, ?> userDelegate,
      final DelayedAsyncStoreWriter delayedWriter
  ) {
    super(userDelegate);
    this.log = new LogContext(String.format(" async-store [%s-%d]", name, partition))
        .logger(AsyncKeyValueStore.class);
    this.userDelegate = (KeyValueStore<KS, VS>) userDelegate;
    this.delayedWriter = delayedWriter;
  }

  public void executeDelayedWrite(final DelayedWrite<KS, VS> delayedWrite) {
    userDelegate.put(delayedWrite.key(), delayedWrite.value());
  }

  /**
   * The public API that's exposed to users for writing to the store. In an async store, this
   * write is intercepted and will not be immediately executed on the underlying store. All
   * such intercepted writes will be moved to a queue where they wait for the original
   * StreamThread to poll them and complete the write by issuing the intercepted #put on
   * the underlying store.
   * At that time, the StreamThread will bypass this method to avoid further interception,
   * and invoke {@link #executeDelayedWrite} to pass the write directly to the underlying store.
   */
  @Override
  public void put(KS key, VS value) {
    delayedWriter.acceptDelayedWriteToAsyncStore(new DelayedWrite<>(key, value, name()));
  }

  @Override
  public VS putIfAbsent(final KS key, final VS value) {
    final VS oldValue = get(key);
    if (oldValue == null) {
      put(key, value);
    }
    return oldValue;
  }

  @Override
  public void putAll(final List<KeyValue<KS, VS>> entries) {
    for (final KeyValue<KS, VS> kv : entries) {
      put(kv.key, kv.value);
    }
  }

  @Override
  public VS delete(final KS key) {
    final VS oldValue = get(key);
    put(key, null);
    return oldValue;
  }

  public VS get(final KS key) {
    // TODO: eventually we'll need to intercept this as well for proper
    //  read-your-write semantics in async processors
    return userDelegate.get(key);
  }

  @Override
  public String name() {
    return userDelegate.name();
  }

  @Override
  public <R> QueryResult<R> query(
      final Query<R> query,
      final PositionBound positionBound,
      final QueryConfig config
  ) {
    throw new UnsupportedOperationException("IQv2 not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<KS, VS> range(final KS from, final KS to) {
    throw new UnsupportedOperationException("#range is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<KS, VS> reverseRange(final KS from, final KS to) {
    throw new UnsupportedOperationException(
        "#reverseRange is not yet supported with async processing"
    );
  }

  @Override
  public KeyValueIterator<KS, VS> all() {
    throw new UnsupportedOperationException(
        "#all is not yet supported with async processing"
    );
  }

  @Override
  public KeyValueIterator<KS, VS> reverseAll() {
    throw new UnsupportedOperationException(
        "#reverseAll is not yet supported with async processing"
    );
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<KS, VS> prefixScan(
      final P prefix,
      final PS prefixKeySerializer
  ) {
    throw new UnsupportedOperationException(
        "#prefixScan is not yet supported with async processing"
    );
  }

  @Override
  public long approximateNumEntries() {
    return userDelegate.approximateNumEntries();
  }

}
