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
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.queues.WriteOnlyProcessingQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool {

  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final LogContext logPrefix;
  private final Logger log;

  private final String streamThreadName;
  private final QueuedEvents queuedEvents;

  // TODO: start up new threads when existing ones die to maintain the target size
  private final int threadPoolSize;
  private final Map<String, AsyncThread> threadPool;
  private final WriteOnlyProcessingQueue processingQueue;

  public AsyncThreadPool(
      final String streamThreadName,
      final int threadPoolSize,
      final int maxQueuedEvents
  ) {
    this.queuedEvents = new QueuedEvents(maxQueuedEvents);
    this.streamThreadName = streamThreadName;
    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);

    this.logPrefix = new LogContext(String.format("stream-thread [%s] ", streamThreadName));
    this.log = logPrefix.logger(AsyncThreadPool.class);

    final ProcessingQueue processingQueue = new ProcessingQueue(logPrefix);
    this.processingQueue = processingQueue;
    startThreads(processingQueue);
  }

  private void startThreads(final ProcessingQueue processingQueue) {
    for (int asyncThreadIndex = 0; asyncThreadIndex < threadPoolSize; ++asyncThreadIndex) {
      final String name = generateAsyncThreadName(streamThreadName, asyncThreadIndex);
      final AsyncThread thread = new AsyncThread(name, processingQueue);

      thread.setDaemon(true);
      threadPool.put(name, thread);
    }

    for (final AsyncThread thread : threadPool.values()) {
      thread.start();
    }

    log.info("Started up all {} async threads in this pool", threadPoolSize);
  }

  public <KOut, VOut> void addProcessor(
      final String asyncProcessorName,
      final int partition,
      final AsyncUserProcessorContext<KOut, VOut> userContext,
      final FinalizingQueue finalizingQueue
  ) {
    processingQueue.addPartition(partition);

    for (final AsyncThread thread : threadPool.values()) {
      final AsyncThreadProcessorContext<KOut, VOut> threadLocalContext =
          new AsyncThreadProcessorContext<>(userContext.taskContext());

      final AsyncNodeContainer processorContainer = new AsyncNodeContainer(
          streamThreadName,
          asyncProcessorName,
          partition,
          threadLocalContext,
          finalizingQueue
      );

      thread.addProcessor(processorContainer, threadLocalContext, userContext);
    }
  }

  public void removeProcessor(
      final String asyncProcessorName,
      final int partition
  ) {
    processingQueue.removePartition(partition);

    for (final AsyncThread thread : threadPool.values()) {
      thread.removeProcessor(asyncProcessorName, partition);
    }
  }

  public QueuedEvents pendingEvents() {
    return queuedEvents;
  }

  public void scheduleForProcessing(final int partition, final List<AsyncEvent> events) {
    processingQueue.offer(partition, events);
  }

  /**
   * @return the name for this AsyncThread, formatted by appending the async thread suffix
   *         based on a unique async-thread index i and the base name of the StreamThread
   *         with index n, ie
   *         AsyncThread.getName() --> {clientId}-StreamThread-{n}-AsyncThread-{i}
   */
  private static String generateAsyncThreadName(
      final String streamThreadName,
      final int asyncThreadIndex
  ) {
    return String.format("%s-%s-%d", streamThreadName, ASYNC_THREAD_NAME, asyncThreadIndex);
  }

  /**
   * Starts up a daemon thread that will send the shutdown signal to
   * all AsyncThreads in this pool, then wait for them to join.
   * <p>
   * Should be called by the StreamThread that owns this pool
   * when it is time for the StreamThread itself to shut down.
   * The AsyncThreadPool is tied to the lifecycle of a StreamThread's
   * main consumer, and will be created and closed alongside it.
   */
  public void shutdown() {
    final Thread shutdownThread = shutdownThread();
    shutdownThread.setDaemon(true);
    shutdownThread.start();
  }

  private Thread shutdownThread() {
    return new Thread(() -> {

      log.info("Sending shutdown signal to all async threads in this pool");
      for (final AsyncThread thread : threadPool.values()) {
        thread.close();
      }

      // Close the queue after sending the shutdown signal to all threads to
      // interrupt any threads that are stuck blocking on the queue
      processingQueue.close();

      for (final AsyncThread thread : threadPool.values()) {
        try {
          thread.join();
        } catch (final InterruptedException e) {
          log.warn("Interrupted while waiting on AsyncThread {} to join, will "
                       + "skip waiting for any remaining threads to join and "
                       + "proceed to shutdown", thread.getName());
          Thread.currentThread().interrupt();
        }
      }
    });
  }

}
