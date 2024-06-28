package dev.responsive.kafka.api.async.internals.queues;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.metrics.AsyncProcessorMetricsRecorder;

public class MeteredSchedulingQueue<KIn> implements SchedulingQueue<KIn> {
  private final AsyncProcessorMetricsRecorder metricsRecorder;
  private final SchedulingQueue<KIn> wrapped;

  public MeteredSchedulingQueue(
      final AsyncProcessorMetricsRecorder metricsRecorder,
      final SchedulingQueue<KIn> schedulingQueue
  ) {
    this.metricsRecorder = metricsRecorder;
    this.wrapped = schedulingQueue;
  }

  private void recordQueueSizes() {
    metricsRecorder.recordSchedulingQueueSize(wrapped.totalEnqueuedEvents());
    metricsRecorder.recordSchedulingQueueLongestSize(wrapped.longestQueueSize());
  }

  public void offer(final AsyncEvent event) {
    wrapped.offer(event);
    recordQueueSizes();
  }

  public AsyncEvent poll() {
    try {
      return wrapped.poll();
    } finally {
      recordQueueSizes();
    }
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public int totalEnqueuedEvents() {
    return wrapped.totalEnqueuedEvents();
  }

  @Override
  public int longestQueueSize() {
    return wrapped.longestQueueSize();
  }

  @Override
  public void unblockKey(KIn key) {
    wrapped.unblockKey(key);
    recordQueueSizes();
  }

  @Override
  public boolean hasProcessableRecord() {
    return wrapped.hasProcessableRecord();
  }

  @Override
  public boolean keyQueueIsFull(KIn key) {
    return wrapped.keyQueueIsFull(key);
  }
}
