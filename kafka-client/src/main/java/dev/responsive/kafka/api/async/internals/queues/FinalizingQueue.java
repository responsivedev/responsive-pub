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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A queue for events whose inputs have been fully processed by the AsyncThread
 * and have been handed off to await the StreamThread for finalization. The
 * StreamThread picks up events from this queue in order to execute any outputs
 * that were intercepted during processing, such as forwards and writes, before
 * ultimating marking the async event as completed. This queue is conceptually
 * the reverse of the thread pool's queue in that it forms a channel from
 * the thread pool threads to the StreamThread, whereas the pool's queue does
 * the exact opposite.
 * <p>
 * Events in this queue are in the {@link AsyncEvent.State#TO_FINALIZE} state
 * <p>
 * Threading notes:
 * -Thread pool --> AsyncThread (see {@link WriteOnlyFinalizingQueue})
 * -Consumes from queue --> StreamThread (see {@link ReadOnlyFinalizingQueue})
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class FinalizingQueue implements ReadOnlyFinalizingQueue, WriteOnlyFinalizingQueue {

  private final Logger log;
  private final BlockingDeque<AsyncEvent> finalizableRecords = new LinkedBlockingDeque<>();
  private final int partition;

  public FinalizingQueue(final String logPrefix, final int partition) {
    this.log = new LogContext(logPrefix).logger(FinalizingQueue.class);
    this.partition = partition;
  }

  /**
   * See {@link WriteOnlyFinalizingQueue#addFinalizableEvent}
   */
  @Override
  public void addFinalizableEvent(
      final AsyncEvent event
  ) {
    if (event.partition() != this.partition) {
      final String errorMsg = String.format(
          "Attempted to finalize an event for partition %d on queue for partition %d",
          event.partition(),
          this.partition
      );
      log.error(errorMsg);
      throw new IllegalStateException(errorMsg);
    }

    finalizableRecords.addLast(event);
  }

  /**
   * See {@link WriteOnlyFinalizingQueue#addFailedEvent}
   */
  @Override
  public void addFailedEvent(
      final AsyncEvent event,
      final Throwable throwable
  ) {
    // Failed events get to jump the line to make sure the StreamThread fails fast
    finalizableRecords.addFirst(event);
  }

  /**
   * See {@link ReadOnlyFinalizingQueue#nextFinalizableEvent()}
   * <p>
   * Note: non-blocking API
   */
  @Override
  public AsyncEvent nextFinalizableEvent() {
    return finalizableRecords.poll();
  }

  /**
   * See {@link ReadOnlyFinalizingQueue#waitForNextFinalizableEvent}
   * <p>
   * Note: blocking API
   */
  @Override
  public AsyncEvent waitForNextFinalizableEvent(long timeout, TimeUnit unit)
      throws InterruptedException {
    return finalizableRecords.poll(timeout, unit);
  }

  /**
   * See {@link ReadOnlyFinalizingQueue#isEmpty()}
   */
  @Override
  public boolean isEmpty() {
    return finalizableRecords.isEmpty();
  }

}

