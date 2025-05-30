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

package dev.responsive.kafka.internal.async.queues;

import dev.responsive.kafka.internal.async.events.AsyncEvent;
import dev.responsive.kafka.internal.async.metrics.AsyncProcessorMetricsRecorder;

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
