/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.api.async.internals.queues;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.util.concurrent.TimeUnit;

/**
 * A read-only version of the {@link FinalizingQueue} intended for the
 * StreamThread to access (by way of the {@link AsyncProcessor}.
 * <p>
 * This queue has both blocking and non-blocking versions of the read
 * API so the StreamThread can finalize events as they finish processing
 * during normal operation, but wait on the events being processed to
 * be added so it can block instead of busy-waiting during the
 * flush/commit procedure.
 * <p>
 * See {@link FinalizingQueue} for full javadocs, and
 * {@link WriteOnlyFinalizingQueue} for the write-only version of this queue
 */
public interface ReadOnlyFinalizingQueue {

  /**
   * Non-blocking read API that returns an event only when one is
   * already available in the queue
   *
   * @return the next finalizable event, or null if there are none
   */
  AsyncEvent nextFinalizableEvent();

  /**
   * Blocking read API that waits if necessary until the queue is
   * non-empty then returns the next event in FIFO order.
   *
   * @return the next finalizable event, blocking if necessary until
   *         onr is available. Guaranteed to never return null
   * @throws InterruptedException if interrupted while blocking
   */
  AsyncEvent waitForNextFinalizableEvent(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * @return true if there are no currently-available events that are
   * awaiting finalization in this queue
   */
  boolean isEmpty();

}
