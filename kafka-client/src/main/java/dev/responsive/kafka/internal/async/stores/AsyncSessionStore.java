/*
 * Copyright 2025 Responsive Computing, Inc.
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class AsyncSessionStore<KS, VS> extends WrappedStateStore<SessionStore<?, ?>, KS, VS>
    implements SessionStore<KS, VS>, AsyncStateStore<KS, VS> {

  private final SessionStore<KS, VS> userDelegate;
  private final DelayedAsyncStoreWriter delayedWriter;

  @SuppressWarnings("unchecked")
  public AsyncSessionStore(
      final String name,
      final int partition,
      final SessionStore<?, ?> userDelegate,
      final DelayedAsyncStoreWriter delayedWriter
  ) {
    super(userDelegate);
    this.userDelegate = (SessionStore<KS, VS>) userDelegate;
    this.delayedWriter = delayedWriter;
  }

  @Override
  public void executeDelayedWrite(final DelayedWrite<KS, VS> delayedWrite) {
    userDelegate.put(delayedWrite.sessionKey(), delayedWrite.value());
  }

  @Override
  public void put(final Windowed<KS> sessionKey, final VS aggregate) {
    delayedWriter.acceptDelayedWriteToAsyncStore(
        DelayedWrite.newSessionWrite(name(), sessionKey, aggregate));
  }

  @Override
  public void remove(final Windowed<KS> sessionKey) {
    put(sessionKey, null);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> fetch(final KS key) {
    return userDelegate.fetch(key);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> fetch(final KS keyFrom, final KS keyTo) {
    return userDelegate.fetch(keyFrom, keyTo);
  }
}
