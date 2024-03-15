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

import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.queues.WritingQueue;
import dev.responsive.kafka.api.async.internals.records.AsyncEvent;
import dev.responsive.kafka.api.async.internals.records.DelayedWrite;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
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
public class AsyncKeyValueStore<KS, VS> implements KeyValueStore<KS, VS> {

  private final Logger log;

  private final String name;
  private final KeyValueStore<KS, VS> userDelegate;

  @SuppressWarnings("unchecked")
  public AsyncKeyValueStore(
      final String name,
      final int partition,
      final KeyValueStore<?, ?> userDelegate
  ) {
    this.log = new LogContext(String.format(" async-store [%s-%d]", name, partition))
        .logger(AsyncKeyValueStore.class);

    this.name = name;
    this.userDelegate = (KeyValueStore<KS, VS>) userDelegate;
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
    final Thread currentThread = Thread.currentThread();

    if (currentThread instanceof AsyncThread) {
      final AsyncEvent<?, ?> currentAsyncEvent =
          ((AsyncThread) currentThread).context().currentAsyncEvent();

      currentAsyncEvent.addWrittenRecord(new DelayedWrite<>(key, value, name()));
    } else {
      log.error("A non-AsyncThread invoked put on this async store. Caller thread name was {}",
                currentThread.getName());
      // The most common reason this case might be hit, besides a bug in the async framework itself,
      // is if users attempt to insert into a state store from other Processor methods, like
      // #init or #close
      throw new IllegalStateException("Can only call #put on an async state store inside the "
                                          + "#process method of an async processor");
    }
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
  @Deprecated
  public void init(
      final org.apache.kafka.streams.processor.ProcessorContext context,
      final StateStore root
  ) {
    throw new UnsupportedOperationException("This init method is deprecated, please implement"
                                                + "init(StateStoreContext, StateStore) instead");
  }

  @Override
  public void init(final StateStoreContext context, final StateStore root) {
    userDelegate.init(context, root);
  }

  @Override
  public void flush() {
    // TODO: how should we handle this, particularly in the ALOS case where it might be
    //  called as part of regular processing?
    userDelegate.flush();
  }

  @Override
  public void close() {
    userDelegate.close();
  }

  @Override
  public boolean persistent() {
    return userDelegate.persistent();
  }

  @Override
  public boolean isOpen() {
    return userDelegate.isOpen();
  }

  @Override
  public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
    throw new UnsupportedOperationException("IQv2 not yet supported with async processing");
  }

  @Override
  public Position getPosition() {
    return userDelegate.getPosition();
  }

  @Override
  public KeyValueIterator<KS, VS> range(final KS from, final KS to) {
    throw new UnsupportedOperationException("#range is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<KS, VS> reverseRange(final KS from, final KS to) {
    throw new UnsupportedOperationException("#reverseRange is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<KS, VS> all() {
    throw new UnsupportedOperationException("#all is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<KS, VS> reverseAll() {
    throw new UnsupportedOperationException("#reverseAll is not yet supported with async processing");
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<KS, VS> prefixScan(final P prefix, final PS prefixKeySerializer) {
    throw new UnsupportedOperationException("#prefixScan is not yet supported with async processing");
  }

  @Override
  public long approximateNumEntries() {
    return userDelegate.approximateNumEntries();
  }

}
