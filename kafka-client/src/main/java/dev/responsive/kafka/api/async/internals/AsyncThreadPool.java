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

import static dev.responsive.kafka.internal.utils.Utils.extractStreamThreadIndex;

import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.MultiplexBlockingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.slf4j.Logger;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool {

  private final Logger log;

  // TODO: start up new threads when existing ones die to maintain the target size
  private final int threadPoolSize;
  private final String streamThreadName;
  private final Map<String, AsyncThread> threadPool;

  public AsyncThreadPool(
      final String streamThreadName,
      final int threadPoolSize
  ) {
    this.streamThreadName = streamThreadName;
    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);
    this.log = new LogContext(String.format(
        "stream-thread [%s]", streamThreadName)
    ).logger(AsyncThreadPool.class);
  }

  public void start() {
    final String streamThreadIndex = extractStreamThreadIndex(streamThreadName);

    for (int i = 0; i < threadPoolSize; ++i) {
      final String name = generateAsyncThreadName(streamThreadIndex, i);
      final AsyncThread thread = new AsyncThread(name);

      thread.setDaemon(true);
      threadPool.put(name, thread);
    }

    for (final AsyncThread thread : threadPool.values()) {
      thread.start();
    }

    log.info("Started up all {} async threads in this pool", threadPoolSize);
  }

  public void addProcessor(
      final int partition,
      final String asyncProcessorName,
      final InternalProcessorContext<?, ?> originalContext,
      final MultiplexBlockingQueue processingQueue,
      final FinalizingQueue<?, ?> finalizingQueue
  ) {
    final AsyncProcessorContainer processorContainer = new AsyncProcessorContainer(
        streamThreadName,
        partition,
        asyncProcessorName,
        new AsyncThreadProcessorContext<>(originalContext),
        processingQueue,
        finalizingQueue
    );

    for (final AsyncThread thread : threadPool.values()) {
      thread.addProcessor(processorContainer);
    }

  }

  public void removeProcessor(
      final int partition,
      final String asyncProcessorName
  ) {
    for (final AsyncThread thread : threadPool.values()) {
      thread.removeProcessor(partition, asyncProcessorName);
    }
  }

  private static String generateAsyncThreadName(
      final String streamThreadIndex,
      final int asyncThreadIndex
  ) {
    return String.format("AsyncThread-%s-%d", streamThreadIndex, asyncThreadIndex);
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
