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

import java.time.Instant;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

public class AsyncFlushingSessionStore 
    extends AbstractAsyncFlushingStore<SessionStore<Bytes, byte[]>>
    implements SessionStore<Bytes, byte[]> {

  public AsyncFlushingSessionStore(
      final SessionStore<Bytes, byte[]> inner,
      final StreamThreadFlushListeners flushListeners
  ) {
    super(inner, flushListeners);
  }
  
  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final long earliestSessionEndTime,
      final long latestSessionEndTime
  ) {
    return wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final Bytes key,
      final long earliestSessionEndTime,
      final long latestSessionStartTime
  ) {
    return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final Bytes key,
      final Instant earliestSessionEndTime,
      final Instant latestSessionStartTime
  ) {
    return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(
      final Bytes key,
      final long earliestSessionEndTime,
      final long latestSessionStartTime
  ) {
    return wrapped().backwardFindSessions(
        key, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(
      final Bytes key,
      final Instant earliestSessionEndTime,
      final Instant latestSessionStartTime
  ) {
    return wrapped().backwardFindSessions(
        key, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long earliestSessionEndTime,
      final long latestSessionStartTime
  ) {
    return wrapped().findSessions(
        keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(
      final Bytes keyFrom,
      final Bytes keyTo,
      final Instant earliestSessionEndTime,
      final Instant latestSessionStartTime
  ) {
    return wrapped().findSessions(
        keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long earliestSessionEndTime,
      final long latestSessionStartTime
  ) {
    return wrapped().backwardFindSessions(
        keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(
      final Bytes keyFrom,
      final Bytes keyTo,
      final Instant earliestSessionEndTime,
      final Instant latestSessionStartTime
  ) {
    return wrapped().backwardFindSessions(
        keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
    );
  }

  @Override
  public byte[] fetchSession(
      final Bytes key,
      final long sessionStartTime,
      final long sessionEndTime
  ) {
    return wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
  }

  @Override
  public byte[] fetchSession(
      final Bytes key,
      final Instant sessionStartTime,
      final Instant sessionEndTime
  ) {
    return wrapped().fetchSession(key, sessionStartTime, sessionEndTime);
  }

  @Override
  public void remove(final Windowed<Bytes> windowedKey) {
    wrapped().remove(windowedKey);
  }

  @Override
  public void put(final Windowed<Bytes> windowedKey, final byte[] value) {
    wrapped().put(windowedKey, value);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
    return wrapped().fetch(key);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
    return wrapped().backwardFetch(key);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
    return wrapped().fetch(keyFrom, keyTo);
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo
  ) {
    return wrapped().backwardFetch(keyFrom, keyTo);
  }
}
