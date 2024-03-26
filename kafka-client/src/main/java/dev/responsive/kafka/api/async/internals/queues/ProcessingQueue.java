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

  private final Map<Integer, Queue<AsyncEvent>> partitions;

  private volatile boolean closed;

  public ProcessingQueue(final LogContext logPrefix) {
    this.log = logPrefix.logger(ProcessingQueue.class);

    // could use per-partition or read-write locks but reads are fast so it's probably not worth it
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();

    this.partitions = new ConcurrentHashMap<>();
  }

  /** See {@link WriteOnlyProcessingQueue#addPartition(int)} */
  @Override
  public void addPartition(final int partition) {
    partitions.put(partition, new LinkedList<>());
  }

  /** See {@link WriteOnlyProcessingQueue#removePartition(int)} */
  @Override
  public void removePartition(final int partition) {
    final var removedPartitionQueue = partitions.remove(partition);
    if (!removedPartitionQueue.isEmpty()) {
      log.warn("Found {} unprocessed events when removing async queue for partition {}",
               removedPartitionQueue.size(), partition);
    }
  }

  /** See {@link ReadOnlyProcessingQueue#take()} */
  @Override
  public AsyncEvent take() throws InterruptedException {
    while (!closed) {

    }
    log.info("Processing queue was closed while waiting for an event");
    return null;
  }

  /** See {@link WriteOnlyProcessingQueue#offer(AsyncEvent)} */
  @Override
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

}
