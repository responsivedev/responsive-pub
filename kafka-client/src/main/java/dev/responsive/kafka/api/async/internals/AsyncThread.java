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

package dev.responsive.kafka.api.async.internals;

import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;

public class AsyncThread extends Thread implements Closeable {

  private final Logger log;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  // TODO: once we allow multiple async processors per topology, this
  //  should map processor name to the map of partition to container
  private final Map<Integer, AsyncProcessorContainer> partitionToAsyncProcessor;

  public AsyncThread(final String name) {
    super(name);
    // must be concurrent b/c new processors are added by StreamThread
    this.partitionToAsyncProcessor = new ConcurrentHashMap<>();
    this.log = new LogContext(String.format("async-thread [%s] ", name)).logger(AsyncThread.class);
  }

  public void addProcessor(
      final AsyncProcessorContainer processorContainer
  ) {
    partitionToAsyncProcessor.put(processorContainer.partition(), processorContainer);
  }

  public void removeProcessor(
      final int partition,
      final String asyncProcessorName
  ) {
    partitionToAsyncProcessor.remove(partition);
  }

  // TODO: once thread pool and processor are decoupled, this should look up the
  //  context in the map by processor node name
  @SuppressWarnings("unchecked")
  public <KOut, VOut> AsyncThreadProcessorContext<KOut, VOut> context(final int partition) {
    final var context = partitionToAsyncProcessor.get(partition).asyncContext();
    return (AsyncThreadProcessorContext<KOut, VOut>) context;
  }

  @Override
  public void run() {
    try {
      while (!shutdownRequested.getOpaque()) {
        processOneEvent();
      }
    } catch (final Exception e) {
      if (shutdownRequested.getOpaque()) {
        log.info("An exception was thrown during shutdown, possibly as part of the shutdown", e);
      } else {
        log.error("Shutting down early due to exception thrown during async processing", e);
      }
    }
  }

  /**
   * A single iteration of the main processing loop for an AsyncThread. The steps are as follows:
   * 1. Pull the next available event that can be processed from the queue (blocking if necessary)
   * 2. Prepare the async context for processing, which entails things like:
   *    - updating its "current async event" to point to the new event
   *    - (re)setting internal metadata used by Streams like the record context
   *       and current node and accessible
   *    - (re)setting any internal state exposed to users that might be requested inside #process
   * 3. Execute the #process method on the input record for this event
   * 4. Complete the processing phase by handing the event to the finalizing queue where the
   *    StreamThread will execute any output/side effects that were created during #process,
   *    such as issuing forwards or writes
   */
  private void processOneEvent() {
    final AsyncEvent<?, ?> currentEvent = processingQueue.nextProcessableEvent();

    asyncContext.prepareToProcessNewEvent(currentEvent);

    currentEvent.inputRecordProcessor().run();

    finalizingQueue.scheduleForFinalization(currentEvent);
  }

  /**
   * AsyncThreads are daemons and hold no resources, so it is technically
   * not an issue if they are, which can occur in the case of
   * a StreamThread that dies during processing. . or wait for all threads to join
   */
  @Override
  public void close() {
    shutdownRequested.setOpaque(true);
  }
}
