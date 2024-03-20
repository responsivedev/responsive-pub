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
public class FinalizingQueue<KIn, VIn> {

  private final Logger log;

  private final BlockingQueue<AsyncEvent<KIn, VIn>> finalizableRecords = new LinkedBlockingQueue<>();

  public FinalizingQueue(final String asyncProcessorName, final int partition) {
    this.log = new LogContext(
        String.format("finalizing-queue [%s-%d]", asyncProcessorName, partition)
    ).logger(dev.responsive.kafka.api.async.internals.queues.ProcessingQueue.class);
  }

  /**
   * Schedules a record that the AsyncThread finished processing and inserts it
   * into the queue for the StreamThread to pick up for finalization
   * <p>
   * To be executed by the StreamThread only
   */
  @SuppressWarnings("unchecked")
  public void scheduleForFinalization(
      final AsyncEvent<?, ?> processedEvent
  ) {
    // Transition to OUTPUT_READY to signal that the event is done with processing
    // and is currently awaiting finalization by the StreamThread
    processedEvent.transitionToOutputReady();

    try {
      finalizableRecords.put((AsyncEvent<KIn, VIn>) processedEvent);
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      log.info("Interrupted while attempting to schedule an event for finalization");
      throw new RuntimeException("Interrupt triggered when passing a processed event back to"
                                     + "the StreamThread for finalization");
    }
  }

  /**
   * Returns the next processable async event or blocks until one becomes available
   * if the queue is empty.
   * <p>
   * To be executed by an AsyncThread only.
   */
  public AsyncEvent<KIn, VIn> nextFinalizableEvent() {
    try {
      return finalizableRecords.take();
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      log.info("Interrupted while waiting to poll the next processed event");
      throw new RuntimeException("Interrupted during blocking poll");
    }
  }

  public boolean isEmpty() {
    return finalizableRecords.isEmpty();
  }

  public ReadOnlyFinalizingQueue<KIn, VIn> readOnly() {
    return new ReadOnlyFinalizingQueue<>(this);
  }

  public WriteOnlyFinalizingQueue<KIn, VIn> writeOnly() {
    return new WriteOnlyFinalizingQueue<>(this);
  }

  public static class ReadOnlyFinalizingQueue<KIn, VIn> {
    private final FinalizingQueue<KIn, VIn> delegate;

    public ReadOnlyFinalizingQueue(FinalizingQueue<KIn, VIn> delegate) {
      this.delegate = delegate;
    }

    public AsyncEvent<KIn, VIn> nextFinalizableEvent() {
      return delegate.nextFinalizableEvent();
    }

    public boolean isEmpty() {
      return delegate.isEmpty();
    }
  }

  public static class WriteOnlyFinalizingQueue<KIn, VIn> {
    private final FinalizingQueue<KIn, VIn> delegate;

    public WriteOnlyFinalizingQueue(FinalizingQueue<KIn, VIn> delegate) {
      this.delegate = delegate;
    }

    public void scheduleForFinalization(final AsyncEvent<?, ?> processedEvent) {
      this.delegate.scheduleForFinalization(processedEvent);
    }
  }
}

