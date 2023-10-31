/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.db.StampedKeySpec;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

// TODO: consider consolidating this with the LocateRemoteKvIterator which does
//  basically the same exact thing but with one remote partition iterator and
//  one local commit buffer iterator, assuming we can extract all the commit buffer
//  specific implementation details (which should probably be done anyways)
public class MultiPartitionRangeIterator<K, V> implements KeyValueIterator<K, V> {

  private final Comparator<K> compareKeys;
  private final PriorityQueue<NextResult> nextResults = new PriorityQueue<>();

  @SuppressWarnings("unchecked")
  public MultiPartitionRangeIterator(final List<KeyValueIterator<K, V>> delegates) {
    K firstKey = null;
    for (final var iter : delegates) {
      if (iter.hasNext()) {
        firstKey = iter.peekNextKey();
        break;
      }
    }
    // TODO: once we hardcode Stamped<K> as Stamped<Bytes> we can just use <K extends Comparable>
    //  here and remove the explicit comparator param, this ridiculously hacky way to initialize
    //  the comparator, and consolidate the two loops above & below this into a single one
    if (firstKey != null) {
      if (firstKey instanceof Bytes) {
        compareKeys = (Comparator<K>) new BytesKeySpec();
      } else if (firstKey instanceof Stamped) {
        compareKeys = (Comparator<K>) new StampedKeySpec(null);
      } else {
        throw new IllegalStateException("Unrecognized key type when we expected only Bytes or"
                                            + " Stamped. Got " + peekNextKey().getClass());
      }
    } else {
      // If there aren't any results, we'll never call the comparator
      compareKeys = null;
    }

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
      return compareKeys.compare(result.key, o.result.key);
    }
  }
}
