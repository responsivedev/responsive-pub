package dev.responsive.kafka.api.async.internals.queues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple class for keeping track of how many events are queued up across
 * all async processors on a given StreamThread. Used to limit the total
 * number of events that each StreamThread can have queued up at one time
 * between all of the processors currently assigned to it.
 * <p>
 * An event is considered "queued" for the duration of its life prior to
 * being processed by the async threads. In other words, from the moment
 * it enters the SchedulingQueue to the moment it leaves the processing
 * queue of the AsyncThreadPool (ie its ExecutorService's task queue).
 * <p>
 * Threading notes:
 * -one per StreamThread
 * -only the #dequeueEvent is called by async threads,
 *   everything else is called from StreamThread
 */
public class QueuedEvents {

  private final int maxQueuedEvents;

  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  // TODO: once we support multiple async processors per Topology, need to account
  //  for async processor name in addition to partition
  private final Map<Integer, AtomicInteger> partitionToNumQueuedEvents = new HashMap<>();
  private int totalQueuedEvents = 0;

  public QueuedEvents(final int maxQueuedEvents) {
    this.maxQueuedEvents = maxQueuedEvents;
  }

  /**
   * Register a new partition.
   */
  public void registerPartition(final int partition) {
    try {
      lock.lock();

      final var oldVal = partitionToNumQueuedEvents.put(partition, new AtomicInteger(0));
      if (oldVal != null) {
        throw new IllegalStateException("Attempted to register existing partition: " + partition);
      }

    } finally {
      lock.unlock();
    }
  }

  /**
   * Unregister a partition and dequeue any remaining events.
   */
  public void unregisterPartition(final int partition) {
    try {
      lock.lock();

      final AtomicInteger numEventsForPartition = partitionToNumQueuedEvents.remove(partition);
      if (numEventsForPartition == null) {
        throw new IllegalStateException("Attempted to unregister non-existing partition: "
                                            + partition);
      }

      totalQueuedEvents -= numEventsForPartition.getOpaque();

    } finally {
      lock.unlock();
    }
  }

  /**
   * Invoked when a StreamThread passes in a new record to the async processor and
   * gets ready to queue up a new pending event. Will block until the number of
   * queued up events falls below the configured threshold
   */
  public void enqueueEvent(final int partition) {
    while (!canEnqueue()) {
      try {
        lock.lock();

        if (!canEnqueue()) {
          condition.await();
        }

        if (canEnqueue()) {
          final AtomicInteger numEventsForPartition = numEventsForPartition(partition, "enqueue");
          numEventsForPartition.getAndAdd(1);
          ++totalQueuedEvents;
        }

      } catch (final InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting to enqueue new record", e);
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Invoked when an async thread dequeues an event to begin processing.
   */
  public void dequeueEvent(final int partition) {
    try {
      lock.lock();

      final AtomicInteger numEventsForPartition = numEventsForPartition(partition, "dequeue");
      numEventsForPartition.getAndAdd(-1);
      --totalQueuedEvents;

      condition.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return true iff there is room to enqueue additional events,
   *         false iff the number of queued events is at/beyond the configured limit
   */
  private boolean canEnqueue() {
    return totalQueuedEvents < maxQueuedEvents;
  }

  private AtomicInteger numEventsForPartition(final int partition, final String action) {
    final AtomicInteger numEventsForPartition = partitionToNumQueuedEvents.get(partition);
    if (numEventsForPartition == null) {
      throw new IllegalStateException(String.format("Attempted to %s for partition %d but it was "
                                                        + "not registered", action, partition));
    }
    return numEventsForPartition;
  }

}
