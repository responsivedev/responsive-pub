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

package dev.responsive.kafka.internal.utils;

import java.util.List;
import java.util.PriorityQueue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

// TODO: consider consolidating this with the LocateRemoteKvIterator which does
//  basically the same exact thing but with one remote partition iterator and
//  one local commit buffer iterator, assuming we can extract all the commit buffer
//  specific implementation details (which should probably be done anyways)
public class MultiPartitionRangeIterator<K extends Comparable<K>, V>
    implements KeyValueIterator<K, V> {

  private final PriorityQueue<NextResult> nextResults = new PriorityQueue<>();

  public MultiPartitionRangeIterator(final List<KeyValueIterator<K, V>> delegates) {
    for (final var iter : delegates) {
      if (iter.hasNext()) {
        final NextResult next = new NextResult(iter.next(), iter);
        nextResults.offer(next);
      }
    }
  }

  @Override
  public void close() {
    while (!nextResults.isEmpty()) {
      nextResults.poll().iterator.close();
    }
  }

  @Override
  public K peekNextKey() {
    return nextResults.peek().result.key;
  }

  @Override
  public boolean hasNext() {
    return !nextResults.isEmpty();
  }

  @Override
  public KeyValue<K, V> next() {
    final NextResult next = nextResults.poll();
    final var iter = next.iterator;
    if (iter.hasNext()) {
      nextResults.offer(new NextResult(iter.next(), iter));
    } else {
      iter.close();
    }
    return next.result;
  }

  private class NextResult implements Comparable<NextResult> {
    final KeyValue<K, V> result;
    final KeyValueIterator<K, V> iterator;

    private NextResult(final KeyValue<K, V> result, final KeyValueIterator<K, V> iterator) {
      this.result = result;
      this.iterator = iterator;
    }

    @Override
    public int compareTo(final NextResult o) {
      return result.key.compareTo(o.result.key);
    }
  }
}
