/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * This class stitches together a remote Cassandra store with a local
 * {@link CommitBuffer} of uncommitted data such that the local data
 * takes precedence over the remote store as the source of truth. This
 * class also "resolves" tombstones in the buffer so that remote data
 * that is deleted is not returned.
 *
 * <p>It is expected that both input iterators return data in the same
 * order (ascending).
 *
 * @implNote this class is kept package private in this package opposed
 *           to moving it into {@link Iterators} since
 *           it requires detailed knowledge about {@link CommitBuffer} and
 *           the way that it works
 */
class LocalRemoteKvIterator<K extends Comparable<K>> implements KeyValueIterator<K, byte[]> {

  private final KeyValueIterator<K, Result<K>> buffered;
  private final KeyValueIterator<K, byte[]> remote;

  // whether the underlying state store can have multiple values for the same key
  private final boolean retainDuplicates;

  private KeyValue<K, byte[]> next;

  public LocalRemoteKvIterator(
      final KeyValueIterator<K, Result<K>> buffered,
      final KeyValueIterator<K, byte[]> remote
  ) {
    this(buffered, remote, false);
  }

  public LocalRemoteKvIterator(
      final KeyValueIterator<K, Result<K>> buffered,
      final KeyValueIterator<K, byte[]> remote,
      final boolean retainDuplicates
  ) {
    this.remote = remote;
    this.buffered = buffered;
    this.retainDuplicates = retainDuplicates;
    next = null;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public K peekNextKey() {
    if (next == null) {
      cache();
    }

    return next.key;
  }

  @Override
  public boolean hasNext() {
    cache();
    return next != null;
  }

  @Override
  public KeyValue<K, byte[]> next() {
    cache();

    final KeyValue<K, byte[]> result = next;
    next = null;
    return result;
  }

  private void cache() {
    if (next == null) {
      next = advance();
    }
  }

  private KeyValue<K, byte[]> advance() {
    if (!remote.hasNext() && !buffered.hasNext()) {
      // this base case is when we've exhausted
      // both iterators and there's nothing left
      // to return
      return null;
    }

    if (!remote.hasNext()) {
      // we just keep cracking along the cached
      // iterator, ignoring tombstones as we won't
      // need to use them
      final KeyValue<K, Result<K>> result = buffered.next();
      if (result.value.isTombstone) {
        return advance();
      }
      return new KeyValue<>(result.key, result.value.value);
    }

    if (!buffered.hasNext()) {
      // we can just return values from the remote
      return remote.next();
    }

    // both iterators have another value to return
    // choose the smaller of the two
    K cachedKey = buffered.peekNextKey();
    K remoteKey = remote.peekNextKey();

    if (remoteKey.compareTo(cachedKey) < 0) {
      // if the remote is smaller, we can just immediately
      // return that
      return remote.next();
    } else if (remoteKey.compareTo(cachedKey) == 0) {

      if (retainDuplicates) {
        // if keys are the same and duplicates are allowed, then we'll need to
        // return both of them eventually. We choose to return the remote one
        // here but it's arbitrary since the order isn't defined, and we'll
        // return the cached one in the next iteration (or once we run out of
        // remote ones since there can be multiple of them)
        return remote.next();
      } else {
        // otherwise if they're the same, then there are two options:
        // (1) the value is a tombstone, in which case we
        // discard both keys and move on or (2) the cached
        // value is more recent, so we return that and advance
        // both - either way, the value from remote.next()
        // should not be returned
        remote.next();
      }
    }

    // return the buffered value, unless it is a tombstone
    // that doesn't exist in remote - in which case we
    // should move on
    final KeyValue<K, Result<K>> result = buffered.next();
    if (result.value.isTombstone) {
      return advance();
    } else {
      return new KeyValue<>(result.key, result.value.value);
    }
  }
}