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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
 * -one per AsyncThreadPool per processor node
 */
public class ProcessingQueue {

  private final Logger log;

  private final Lock lock;
  // condition for writer to signal when new events are added
  private final Condition newEventsCondition;
  // condition for readers to signal when all queues are empty
  private final Condition emptyCondition;


  private final Set<Integer> nonEmptyQueues;
  private final Map<Integer, Queue<AsyncEvent>> partitions;

  public ProcessingQueue(final LogContext logPrefix) {
    this.log = logPrefix.logger(ProcessingQueue.class);

    // could use per-partition or read-write locks but reads are fast so it's probably not worth it
    this.lock = new ReentrantLock();
    this.newEventsCondition = lock.newCondition();
    this.emptyCondition = lock.newCondition();

    this.nonEmptyQueues = new HashSet<>();
    this.partitions = new HashMap<>();
  }

  /**
   * Register a new partition queue.
   * <p>
   * Threading notes:
   * -only invoked by StreamThread
   * -no locking/signalling required since partitions are stored in concurrent map
   *   and we don't add new records in this method, just a new/empty queue
   */
  public void addPartition(final int partition) {
    partitions.put(partition, new LinkedList<>());
  }

  /**
   * Unregister an existing partition queue and remove any scheduled events
   * <p>
   * Threading notes:
   * -only invoked by StreamThread
   * -no locking/signalling required since partitions are stored in concurrent map
   * -removed partitions will be empty anyways, unless we hit an unclean shutdown
   */
  public void removePartition(final int partition) {
    final var removedPartitionQueue = partitions.remove(partition);
    if (!removedPartitionQueue.isEmpty()) {
      log.warn("Found {} unprocessed events when removing async queue for partition {}",
               removedPartitionQueue.size(), partition);
    }
  }

  /**
   * Blocks until all partition-queues are empty or until the timeout is hit.
   *
   * @return true if the queues are indeed empty, false if we returned early due to timeout
   */
  public boolean awaitEmpty(final long timeout, final TimeUnit unit) throws InterruptedException {

    // This method is only available to writers and isEmpty is volatile, so we
    // can return immediately if it's already true as no other threads will add events
    while (!isEmpty) {
      try {
        lock.lock();
        if (!isEmpty) {
          final boolean timeoutHit = !emptyCondition.await(timeout, unit);
          if (timeoutHit) {
            return false;
          }
        }

      } finally {
        lock.unlock();
      }
    }

    return true;
  }

  /**
   * Blocking read API
   *
   * @return the next available event across any partitions
   */
  public AsyncEvent take() throws InterruptedException {

  }

  /**
   * Non-blocking write API. Schedules the given event for processing on the
   * corresponding partition
   */
  public void offer(final AsyncEvent event)  {
    final int partition = event.partition();
    final Queue<AsyncEvent> partitionQueue = partitions.get(partition);

    try {
      lock.lock();

      partitionQueue.offer(event);
      if (isEmpty) {
        isEmpty = false;
      }

    } finally {
      lock.unlock();
    }
  }

}
