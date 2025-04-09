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

import java.time.Instant;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public class AsyncFlushingWindowStore
    extends AbstractAsyncFlushingStore<WindowStore<Bytes, byte[]>>
    implements WindowStore<Bytes, byte[]> {

  public AsyncFlushingWindowStore(
      final WindowStore<Bytes, byte[]> inner,
      final StreamThreadFlushListeners flushListeners
  ) {
    super(inner, flushListeners);
  }

  @Override
  public void put(final Bytes key, final byte[] value, final long timestamp) {
    wrapped().put(key, value, timestamp);
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return wrapped().fetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().fetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return wrapped().backwardFetch(key, timeFrom, timeTo);
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().backwardFetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo, final long timeFrom, final long timeTo
  ) {
    return wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    return wrapped().fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    return wrapped().backwardFetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final Instant timeFrom,
      final Instant timeTo
  ) throws IllegalArgumentException {
    return wrapped().backwardFetchAll(timeFrom, timeTo);
  }

  @Override
  public byte[] fetch(final Bytes key, final long timestamp) {
    return wrapped().fetch(key, timestamp);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    return wrapped().all();
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    return wrapped().backwardAll();
  }
}

