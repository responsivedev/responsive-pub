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

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A queue-like data structure similar (in both design and semantics) to the
 * LinkedBlockingQueue, but with multiple internal queues that correspond to
 * a specific partition and can be added or removed dynamically.
 * <p>
 * This queue assumes single-writer, many-reader semantics, and will serve
 * records in the order that they are inserted into the queue regardless of
 * partition.
 * The queue provides blocking semantics for readers of the queue so they can
 * wait until a record is inserted into one of the partitions.
 * <p>
 * The point of this queue is to multiplex input elements that are inserted
 * by a single thread across all partitions, but pulled from the queue by
 * multiple threads which are agnostic to the partition. This custom queue
 * is necessary because we must keep the internal partitions divided to
 * efficiently add/remove/check entire partitions at once, but also need
 * to provide consumers of the queue with a unified API that allows them
 * to block when all queues are empty and be notified if/when a record is
 * added to any of the underlying partition queues.
 * <p>
 * Threading notes:
 * -written to by StreamThread, read from by AsyncThread(s)
 * -one per physical AsyncProcessor instance
 *  (ie per logical processor per partition per StreamThread)
 * -notably, the same queue is used for all partitions and
 */
public class ProcessingQueue implements ReadOnlyProcessingQueue, WriteOnlyProcessingQueue {

  private final Logger log;

  private final Lock lock;
  private final Condition condition; // signals when new events are added

  private volatile boolean closed;
  // a circular queue of non-empty partitions to pick from next
  private final Queue<PartitionQueue> nonEmptyPartitionQueues = new LinkedList<>();

  // not protected by the lock
  private final Map<Integer, PartitionQueue> allPartitionQueues = new ConcurrentHashMap<>();

  public ProcessingQueue(final LogContext logPrefix) {
    this.log = logPrefix.logger(ProcessingQueue.class);

    // could use per-partition or read-write locks but reads are fast so it's probably not worth it
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();
  }

  /** See {@link WriteOnlyProcessingQueue#addPartition(int)} */
  @Override
  public void addPartition(final int partition) {
    allPartitionQueues.put(partition, new PartitionQueue(partition));
  }

  /** See {@link WriteOnlyProcessingQueue#removePartition(int)} */
  @Override
  public void removePartition(final int partition) {
    final PartitionQueue removedPartitionQueue = allPartitionQueues.remove(partition);
    if (!removedPartitionQueue.isEmpty()) {
      log.warn("Found {} unprocessed events when removing async queue for partition {}",
               removedPartitionQueue.numberOfEvents(), partition);
    }
  }

  /** See {@link ReadOnlyProcessingQueue#take()} */
  @Override
  public AsyncEvent take() throws InterruptedException {
    while (!closed) {
      try {
        lock.lock();
        final PartitionQueue nonEmptyPartitionQueue = nextNonEmptyPartitionQueue();

        if (nonEmptyPartitionQueue == null) {
          condition.await();
        } else {
          final AsyncEvent event = nonEmptyPartitionQueue.nextEvent();
          if (!nonEmptyPartitionQueue.isEmpty()) {
            // reschedule at the back of the queue if there are more events
            nonEmptyPartitionQueues.add(nonEmptyPartitionQueue);
          }

          return event;
        }

      } finally {
        lock.unlock();
      }
    }
    log.info("Processing queue was closed while waiting for an event");
    return null;
  }

  /**
   * Grab the next non-empty queue, if any exist, otherwise return null
   * <p>
   * Callers must have already acquired the lock when invoking this method
   */
  private PartitionQueue nextNonEmptyPartitionQueue() {
    while (!nonEmptyPartitionQueues.isEmpty()) {
      final PartitionQueue partitionQueue = nonEmptyPartitionQueues.poll();

      // It's possible this partition was recently removed, in which case
      // just discard it from the nonEmptyPartitions queue and move on
      if (!allPartitionQueues.containsKey(partitionQueue.partition())) {
        continue;
      // It's possible the queue is actually empty if it was removed and then
      // added back before that partition came up in the nonEmptyPartitions queue
      // This should be infrequent, so we just log a warning
      } else if (partitionQueue.isEmpty()) {
        log.warn("Partition {} was marked non-empty but had no events", partitionQueue.partition());
        continue;
      }
      return partitionQueue;
    }
    return null;
  }

  /** See {@link WriteOnlyProcessingQueue#offer(int, List)} */
  @Override
  public void offer(final int partition, final List<AsyncEvent> events)  {
    final PartitionQueue partitionQueue = allPartitionQueues.get(partition);

    try {
      lock.lock();
      final boolean wasEmptyQueue = partitionQueue.isEmpty();

      for (final AsyncEvent event : events) {
        partitionQueue.addEvent(event);
      }

      if (wasEmptyQueue) {
        nonEmptyPartitionQueues.add(partitionQueue);

        if (events.size() == 1) {
          condition.signal();
        } else {
          // If we scheduled more than one event, just signal all the threads
          condition.signalAll();
        }
      }

    } finally {
      lock.unlock();
    }
  }

  /** See {@link WriteOnlyProcessingQueue#close()} */
  @Override
  public void close() {
    closed = true;
    try {
      lock.lock();
      condition.signalAll();
    } finally {
      lock.unlock();
    }
    log.info("Closed the processing queue and notified waiting threads");
  }

  private static class PartitionQueue {
    private final int partition;
    private final Queue<AsyncEvent> events;

    public PartitionQueue(final int partition) {
      this.partition = partition;
      this.events = new LinkedList<>();
    }

    private int partition() {
      return partition;
    }

    private boolean isEmpty() {
      return events.isEmpty();
    }

    private AsyncEvent nextEvent() {
      return events.poll();
    }

    private void addEvent(final AsyncEvent event) {
      events.add(event);
    }

    private int numberOfEvents() {
      return events.size();
    }
  }

}
