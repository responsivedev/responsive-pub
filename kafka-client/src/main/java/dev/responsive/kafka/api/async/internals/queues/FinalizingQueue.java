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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A queue for events whose inputs have been fully processed by the AsyncThread
 * and have been handed off to await the StreamThread for finalization. The
 * StreamThread picks up events from this queue in order to execute any outputs
 * that were intercepted during processing, such as forwards and writes, before
 * ultimating marking the async event as completed. This queue is conceptually
 * the reverse of the {@link ProcessingQueue} in that it forms a channel from
 * the AsyncThread(s) to the StreamThread, whereas the ProcessingQueue does the
 * exact opposite.
 * <p>
 * Threading notes:
 * -Produces to queue --> AsyncThread
 * -Consumes from queue --> StreamThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class FinalizingQueue {

  private final Logger log;
  private final Queue<AsyncEvent> finalizableRecords = new ConcurrentLinkedQueue<>();

  public FinalizingQueue(final String logPrefix) {
    this.log = new LogContext(logPrefix).logger(FinalizingQueue.class);
  }

  /**
   * Schedules a record that the AsyncThread finished processing and inserts it
   * into the queue for the StreamThread to pick up for finalization
   * <p>
   * To be executed by AsyncThreads only
   */
  public void scheduleForFinalization(
      final AsyncEvent processedEvent
  ) {
    // Transition to OUTPUT_READY to signal that the event is done with processing
    // and is currently awaiting finalization by the StreamThread
    processedEvent.transitionToToFinalize();

    finalizableRecords.add(processedEvent);
  }

  /**
   * @return the next finalizable event, or null if there are none
   * <p>
   * To be executed by the StreamThread only
   */
  public AsyncEvent nextFinalizableEvent() {
    return finalizableRecords.poll();
  }

  public boolean isEmpty() {
    return finalizableRecords.isEmpty();
  }

}

