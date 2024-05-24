package dev.responsive.kafka.api.async.internals.queues;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;

public interface SchedulingQueue<KIn> {
  boolean isEmpty();

  int totalEnqueuedEvents();

  int longestQueueSize();

  void unblockKey(KIn key);

  boolean hasProcessableRecord();

  AsyncEvent poll();

  void offer(AsyncEvent event);


  boolean keyQueueIsFull(KIn key);

  int size();

  int blockedEntries();
}
