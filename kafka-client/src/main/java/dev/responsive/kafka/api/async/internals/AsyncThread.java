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
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.queues.ReadOnlyProcessingQueue;
import java.io.Closeable;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class AsyncThread extends Thread implements Closeable {

  private final Logger log;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  private final ReadOnlyProcessingQueue processingQueue;
  private final ConcurrentMap<Integer, AsyncNodeContainer> nodeIdToContainer;
  private final Queue<UninitializedContext<?, ?>> uninitializedContexts;

  public AsyncThread(final String name, final ProcessingQueue processingQueue) {
    super(name);
    this.log = new LogContext(String.format("async-thread [%s] ", name)).logger(AsyncThread.class);

    this.processingQueue = processingQueue;

    // concurrent b/c new processors are added by StreamThread via AsyncThreadPool
    // Luckily we don't need real synchronization, as long as we drain the uninitializedContexts
    // queue before picking up any new records, since it's guaranteed that the StreamThread
    // will at least have added the new context to the uninitializedContexts queue before any
    // actual records can be scheduled for that processor/partition. This is due to the
    // context being added during the processor's #init method, whereas records won't be
    // scheduled until it's moved on to #process
    this.uninitializedContexts = new ConcurrentLinkedQueue<>();
    this.nodeIdToContainer = new ConcurrentHashMap<>();
  }

  public <KOut, VOut> void addProcessor(
      final AsyncNodeContainer processorContainer,
      final AsyncThreadProcessorContext<KOut, VOut> threadLocalContext,
      final AsyncUserProcessorContext<KOut, VOut> userContext
  ) {
    final int partition = processorContainer.partition();
    final String processorNodeId =
        String.format("%s[%d]", processorContainer.asyncProcessorName(), partition);

    log.debug("Adding processor {}", processorNodeId);
    nodeIdToContainer.put(partition, processorContainer);

    final UninitializedContext<KOut, VOut> uninitializedContext = new UninitializedContext<>(
        log,
        userContext,
        threadLocalContext,
        processorNodeId
    );

    uninitializedContexts.add(uninitializedContext);
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

    // Note: we don't need to remove the context since it'll just get overridden if the task
    // is reassigned sometime in the future
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
   * 0. Pull the next available event that can be processed from the queue (blocking if necessary)
   * 1. Drain the uninitializedContexts queue to make sure all ThreadLocal contexts have been set
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

    initializeAllContexts();

    // This should only happen if the queue was closed or thread was interrupted.
    // In either case, the thread should be shutting down
    if (currentEvent == null) {
      if (!shutdownRequested.getOpaque()) {
        log.warn("Processing queue returned null but thread was not requested to shut down");
        close();
      }
      return;
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

  private void initializeAllContexts() {
    while (!uninitializedContexts.isEmpty()) {
      final var uninitializedContext = uninitializedContexts.poll();
      uninitializedContext.initialize();
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

  // Simple class to wrap all the necessary info and logic we need to
  // initialize a context so we can maintain a simple queue with the
  // ones that need to be initialized
  private static class UninitializedContext<KOut, VOut> {
    private final Logger log;
    private final AsyncUserProcessorContext<KOut, VOut> userContext;
    private final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext;
    private final String processorNodeId;

    public UninitializedContext(
        final Logger log,
        final AsyncUserProcessorContext<KOut, VOut> userContext,
        final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext,
        final String processorNodeId
    ) {
      this.log = log;
      this.userContext = userContext;
      this.asyncThreadContext = asyncThreadContext;
      this.processorNodeId = processorNodeId;
    }

    public void initialize() {
      log.debug("Initializing context for processor {}", processorNodeId);
      userContext.setDelegateForAsyncThread(asyncThreadContext);
    }
  }
}
