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

public interface SchedulingQueue<KIn> {
  boolean isEmpty();

  int totalEnqueuedEvents();

  int longestQueueSize();

  void unblockKey(KIn key);

  boolean hasProcessableRecord();

  AsyncEvent poll();

  void offer(AsyncEvent event);

  boolean keyQueueIsFull(KIn key);
}
