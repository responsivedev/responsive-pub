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
import java.time.Instant;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class AsyncWindowStore<KS, VS> extends WrappedStateStore<WindowStore<?, ?>, KS, VS>
    implements WindowStore<KS, VS>, AsyncStateStore<KS, VS> {

  private final WindowStore<KS, VS> userDelegate;
  private final DelayedAsyncStoreWriter delayedWriter;

  @SuppressWarnings("unchecked")
  public AsyncWindowStore(
      final String name,
      final int partition,
      final WindowStore<?, ?> userDelegate,
      final DelayedAsyncStoreWriter delayedWriter
  ) {
    super(userDelegate);
    this.userDelegate = (WindowStore<KS, VS>) userDelegate;
    this.delayedWriter = delayedWriter;
  }

  @Override
  public void executeDelayedWrite(final DelayedWrite<KS, VS> delayedWrite) {
    userDelegate.put(delayedWrite.key(), delayedWrite.value(), delayedWrite.windowStartMs());
  }

  @Override
  public void put(final KS key, final VS value, final long windowStartMs) {
    delayedWriter.acceptDelayedWriteToAsyncStore(
        DelayedWrite.newWindowWrite(name(), key, value, windowStartMs));
  }

  @Override
  public VS fetch(final KS key, final long windowStartMs) {
    return userDelegate.fetch(key, windowStartMs);
  }

  @Override
  public WindowStoreIterator<VS> fetch(final KS key, final long timeFrom, final long timeTo) {
    return userDelegate.fetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<VS> fetch(final KS key, final Instant timeFrom, final Instant timeTo)
      throws IllegalArgumentException {
    return userDelegate.fetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<VS> backwardFetch(
      final KS key,
      final long timeFrom,
      final long timeTo
  ) {
    return userDelegate.backwardFetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<VS> backwardFetch(
      final KS key,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return userDelegate.backwardFetch(key, timeFrom, timeTo);
  }

  @Override
  @SuppressWarnings("checkstyle:overloadmethodsdeclarationorder")
  public KeyValueIterator<Windowed<KS>, VS> fetch(
      final KS keyFrom,
      final KS keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    return userDelegate.fetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> fetch(
      final KS keyFrom,
      final KS keyTo,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return userDelegate.fetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  @SuppressWarnings("checkstyle:overloadmethodsdeclarationorder")
  public KeyValueIterator<Windowed<KS>, VS> backwardFetch(
      final KS keyFrom,
      final KS keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    return userDelegate.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> backwardFetch(
      final KS keyFrom, final KS keyTo, final Instant timeFrom, final Instant timeTo
  ) throws IllegalArgumentException {
    return userDelegate.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> fetchAll(final long timeFrom, final long timeTo) {
    return userDelegate.fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> fetchAll(final Instant timeFrom, final Instant timeTo)
      throws IllegalArgumentException {
    return userDelegate.fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    return userDelegate.backwardFetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> backwardFetchAll(
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return userDelegate.backwardFetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> all() {
    return userDelegate.all();
  }

  @Override
  public KeyValueIterator<Windowed<KS>, VS> backwardAll() {
    return userDelegate.backwardAll();
  }
}
