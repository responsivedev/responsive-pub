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

import dev.responsive.kafka.api.async.internals.AsyncThread;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A queue for events with processable records that have been scheduled and are waiting to be
 * picked up and executed by an {@link AsyncThread} from the pool. Essentially the
 * input queue to the async processing thread pool
 * <p>
 * Threading notes:
 * -Produces to queue --> StreamThread
 * -Consumes from queue --> AsyncThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class ProcessingQueue<KIn, VIn> {
  private final Logger log;


  private final BlockingQueue<AsyncEvent<KIn, VIn>> processableEvents = new LinkedBlockingQueue<>();

  public ProcessingQueue(final String asyncProcessorName, final int partition) {
    this.log = new LogContext(
        String.format("processing-queue [%s-%d]", asyncProcessorName, partition)
    ).logger(ProcessingQueue.class);
  }

  /**
   * Schedules a processable record to be executed asynchronously by a thread from the async pool
   * <p>
   * To be executed by the StreamThread only
   */
  public void scheduleForProcessing(
      final AsyncEvent<KIn, VIn> processableEvent
  ) {
    try {
      processableEvents.put(processableEvent);
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      log.info("Interrupted while attempting to schedule an event for processing");
      throw new RuntimeException("Interrupt triggered when scheduling a new event");
    }
  }

  /**
   * Returns the next processable async event or blocks until one becomes available
   * if the queue is empty.
   * <p>
   * To be executed by an AsyncThread only.
   */
  public AsyncEvent<KIn, VIn> nextProcessableEvent() {
    try {
      return processableEvents.take();
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      log.info("Interrupted while waiting to poll record");
      throw new RuntimeException("Interrupted during blocking poll");
    }
  }

  public ReadOnlyProcessingQueue<KIn, VIn> readOnly() {
    return new ReadOnlyProcessingQueue<>(this);
  }

  public WriteOnlyProcessingQueue<KIn, VIn> writeOnly() {
    return new WriteOnlyProcessingQueue<>(this);
  }

  public static class ReadOnlyProcessingQueue<KIn, VIn> {
    private final ProcessingQueue<KIn, VIn> delegate;

    public ReadOnlyProcessingQueue(ProcessingQueue<KIn, VIn> delegate) {
      this.delegate = delegate;
    }

    public AsyncEvent<KIn, VIn> nextProcessableEvent() {
      return delegate.nextProcessableEvent();
    }
  }

  public static class WriteOnlyProcessingQueue<KIn, VIn> {
    private final ProcessingQueue<KIn, VIn> delegate;

    public WriteOnlyProcessingQueue(ProcessingQueue<KIn, VIn> delegate) {
      this.delegate = delegate;
    }

    public void scheduleForProcessing(final AsyncEvent<KIn, VIn> processableEvent) {
      delegate.scheduleForProcessing(processableEvent);
    }
  }
}
