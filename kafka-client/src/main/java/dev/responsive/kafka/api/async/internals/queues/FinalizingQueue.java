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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
 * Events in this queue are in the {@link AsyncEvent.State#FINALIZING} state
 * <p>
 * Threading notes:
 * -Thread pool --> AsyncThread (see {@link WriteOnlyFinalizingQueue})
 * -Consumes from queue --> StreamThread (see {@link ReadOnlyFinalizingQueue})
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 *
 * <p> TODO: implement mechanism for AsyncThreads to forward processing errors
 *       to the StreamThread to rethrow
 */
public class FinalizingQueue implements ReadOnlyFinalizingQueue, WriteOnlyFinalizingQueue {

  private final Logger log;
  private final BlockingQueue<AsyncEvent> finalizableRecords = new LinkedBlockingQueue<>();
  private final int partition;

  public FinalizingQueue(final String logPrefix, final int partition) {
    this.log = new LogContext(logPrefix).logger(FinalizingQueue.class);
    this.partition = partition;
  }

  /**
   * See {@link WriteOnlyFinalizingQueue#scheduleForFinalization(AsyncEvent)}
   */
  @Override
  public void scheduleForFinalization(
      final AsyncEvent processedEvent
  ) {
    if (processedEvent.partition() != this.partition) {
      throw new IllegalStateException(String.format(
          "attempted to finalize an event for partition %d on queue for partition %d",
          processedEvent.partition(),
          this.partition
      ));
    }
    // Transition to OUTPUT_READY to signal that the event is done with processing
    // and is currently awaiting finalization by the StreamThread
    processedEvent.transitionToToFinalize();

    finalizableRecords.add(processedEvent);
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
   * See {@link ReadOnlyFinalizingQueue#waitForNextFinalizableEvent()}
   * <p>
   * Note: blocking API
   */
  @Override
  public AsyncEvent waitForNextFinalizableEvent() throws InterruptedException {
    return finalizableRecords.take();
  }

  /**
   * See {@link ReadOnlyFinalizingQueue#isEmpty()}
   */
  @Override
  public boolean isEmpty() {
    return finalizableRecords.isEmpty();
  }

}

