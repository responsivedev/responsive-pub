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
