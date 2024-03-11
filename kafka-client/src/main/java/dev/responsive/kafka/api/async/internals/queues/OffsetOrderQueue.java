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

package dev.responsive.kafka.api.async.internals.queues;

import dev.responsive.kafka.api.async.internals.records.AsyncRecord;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic building-block queue at the heart of most of the async queues.
 * This class is a simple wrapper around the {@link PriorityBlockingQueue}
 * that returns records in offset-and-topic order and provides a choice between
 * blocking and non-blocking semantics.
 * Also allows for an additional comparator in case multiple records may have
 * the same offset and topic value.
 * TODO: implement proper interrupt semantics for clean closing
 */
public class OffsetOrderQueue<K, V, R extends AsyncRecord<K, V>> {

  private static final Logger LOG = LoggerFactory.getLogger(OffsetOrderQueue.class);

  // Based on PriorityBlockingQueue.INITIAL_CAPACITY -- needed for constructor
  private static final int INITIAL_CAPACITY = 11;

  private final boolean blocking;
  private final BlockingQueue<R> processableRecords;

  public static <K, V, R extends AsyncRecord<K, V>> OffsetOrderQueue<K, V, R> blockingQueue(
      final Comparator<R> additionalComparator // may be null
  ) {
    return new OffsetOrderQueue<>(true, additionalComparator);
  }

  public static <K, V, R extends AsyncRecord<K, V>> OffsetOrderQueue<K, V, R> nonBlockingQueue(
      final Comparator<R> additionalComparator // may be null
  ) {
    return new OffsetOrderQueue<>(false, additionalComparator);
  }

  private OffsetOrderQueue(
      final boolean blocking,
      final Comparator<R> additionalComparator
  ) {
    this.blocking = blocking;
    this.processableRecords = new PriorityBlockingQueue<>(
        INITIAL_CAPACITY, offsetOrderComparator(additionalComparator)
    );
  }

  /**
   * @return the record with the next-lowest offset,
   *          if blocking: never returns null, will wait for a record
   *                     or throw an exception if interrupted
   *          if non-blocking: returns null if the queue is empty
   */
  public R poll() {
    if (blocking) {
      try {
        return processableRecords.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        // Just throw an exception for now to ensure shutdown occurs
        LOG.info("Interrupted while waiting to poll record");
        throw new RuntimeException("Interrupted during blocking poll");
      }
    } else {
      return processableRecords.poll();
    }
  }

  public boolean isEmpty() {
    return processableRecords.isEmpty();
  }

  public void put(final R record) {
    try {
      processableRecords.put(record);
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      LOG.info("Interrupted while attempting to insert record");
      throw new RuntimeException("Interrupted during blocking put");
    }
  }

  private static <K, V, R extends AsyncRecord<K, V>> Comparator<R> offsetOrderComparator(
      final Comparator<R> optionalAdditionalComparator
  ) {

    final Comparator<R> comparingOffsets =
        Comparator.comparingLong(AsyncRecord::offset);

    final Comparator<R> comparingOffsetsAndTopics =
        comparingOffsets.thenComparing(AsyncRecord::topic);

    if (optionalAdditionalComparator == null) {
      return comparingOffsetsAndTopics;
    } else {
      return comparingOffsetsAndTopics.thenComparing(optionalAdditionalComparator);
    }
  }
}
