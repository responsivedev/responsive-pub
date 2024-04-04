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
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.queues.ReadOnlyProcessingQueue;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class AsyncThread extends Thread implements Closeable {

  private final Logger log;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  private final ReadOnlyProcessingQueue processingQueue;
  private final Map<Integer, AsyncNodeContainer> nodeIdToContainer;

  public AsyncThread(final String name, final ProcessingQueue processingQueue) {
    super(name);
    this.log = new LogContext(String.format("async-thread [%s] ", name)).logger(AsyncThread.class);

    this.processingQueue = processingQueue;

    // concurrent b/c new processors are added by StreamThread via AsyncThreadPool
    this.nodeIdToContainer = new ConcurrentHashMap<>();
  }

  public void addProcessor(
      final AsyncNodeContainer processorContainer
  ) {
    log.debug("Adding partition {} of processor {}",
              processorContainer.partition(), processorContainer.asyncProcessorName());
    nodeIdToContainer.put(processorContainer.partition(), processorContainer);
  }

  public void removeProcessor(
      final String asyncProcessorName,
      final int partition
  ) {
    final AsyncNodeContainer processorContainer = nodeIdToContainer.remove(partition);
    if (processorContainer == null) {
      log.error("Attempted to remove partition {} of processor {} but it was not found",
                partition, asyncProcessorName);
    }

    log.debug("Removing processor {} for partition {}", asyncProcessorName, partition);
  }

  @SuppressWarnings("unchecked")
  public <KOut, VOut> AsyncThreadProcessorContext<KOut, VOut> context(final int partition) {
    final var container = nodeIdToContainer.get(partition);
    if (container == null) {
      log.error("Unable to locate processor node for partition {}", partition);
      throw new IllegalStateException("Failed to map async context to partition");
    }

    return (AsyncThreadProcessorContext<KOut, VOut>) container.asyncContext();
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
    final AsyncEvent currentEvent = nextProcessableEvent();

    // This should only happen if the queue was closed or thread was interrupted.
    // In either case, the thread should be shutting down
    if (currentEvent == null) {
      if (!shutdownRequested.getOpaque()) {
        log.warn("Processing queue returned null but thread was not requested to shut down");
        close();
        return;
      }
    }

    final AsyncNodeContainer container = nodeIdToContainer.get(currentEvent.partition());

    container.asyncContext().prepareToProcessNewEvent(currentEvent);

    currentEvent.inputRecordProcessor().run();

    container.finalizingQueue().scheduleForFinalization(currentEvent);
  }

  private AsyncEvent nextProcessableEvent() {
    try {
      return processingQueue.take();
    } catch (final InterruptedException e) {
      log.error("Interrupted while waiting for next processable event", e);
      close();
      return null;
    }
  }

  /**
   * Send a shutdown signal to this AsyncThread to make it exit the
   * main processing loop
   */
  @Override
  public void close() {
    log.info("Shutting down async thread");
    shutdownRequested.setOpaque(true);
  }
}
