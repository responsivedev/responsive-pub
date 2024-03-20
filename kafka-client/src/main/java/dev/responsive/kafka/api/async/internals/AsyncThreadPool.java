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
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue.WriteOnlyFinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue.ReadOnlyProcessingQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool {

  private final Logger log;

  // TODO: start up new threads when existing ones die to maintain the target size
  private final int threadPoolSize;
  private final Map<String, AsyncThread> threadPool;

  public AsyncThreadPool(
      final int threadPoolSize,
      final ProcessorContext<?, ?> originalContext,
      final String streamThreadName,
      final ReadOnlyProcessingQueue<?, ?> processableRecords,
      final WriteOnlyFinalizingQueue<?, ?> finalizableRecords
      ) {
    this.log = new LogContext(String.format(
        "stream-thread [%s]", streamThreadName)
    ).logger(AsyncThreadPool.class);

    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);

    final String streamThreadIndex = extractStreamThreadIndex(streamThreadName);

    for (int i = 0; i < threadPoolSize; ++i) {
      final String name = generateAsyncThreadName(streamThreadIndex, i);
      final AsyncThread thread = new AsyncThread(
          name,
          originalContext,
          processableRecords,
          finalizableRecords
      );

      thread.setDaemon(true);

      threadPool.put(name, thread);
    }

    for (final AsyncThread thread : threadPool.values()) {
      thread.start();
    }

    log.info("Started up all {} async threads in this pool", threadPoolSize);
  }

  public Set<String> asyncThreadNames() {
    return threadPool.keySet();
  }

  public <KOut, VOut> AsyncThreadProcessorContext<KOut, VOut> asyncContextForThread(
      final String threadName
  ) {
    return threadPool.get(threadName).context();
  }

  private static String generateAsyncThreadName(
      final String streamThreadIndex,
      final int asyncThreadIndex
  ) {
    return String.format("AsyncThread-%s-%d", streamThreadIndex, asyncThreadIndex);
  }

  /**
   * Shutdown and optionally await all AsyncThreads in this pool. The thread
   * pool should not be closed before the StreamThread that owns it is
   * shut down as well.
   * <p>
   * AsyncThreads are all daemons and hold no resources, so it is technically
   * not an issue if shutdown is not called on an AsyncThreadPool, which is
   * possible if the StreamThread that owns this pool dies during processing
   * or in some rare cases, if the {@link KafkaStreams#close()} timeout is
   * exceeded.
   * It also means we do not need to wait for the threads to join and can
   * simply send the shutdown signal and exit. Note that during a graceful
   * shutdown, all in-flight events for all async processors will have
   * already been completed and committed prior to the StreamThread and
   * AsyncThreadPool being shut down, so the AsyncThreads are likely to
   * be idling and won't take long to rejoin.
   * During an unclean shutdown, the AsyncThreads may remain actively
   * processing new events when the shutdown signal is sent. In this
   * case, it may take longer for the AsyncThreads to rejoin, and their
   * progress since the last commit will be lost anyway, so it is
   * recommended to just send the shutdown signal and return without
   * waiting for them
   */
  public void shutdown(final boolean waitForThreads) {
    for (final AsyncThread thread : threadPool.values()) {
      thread.close();
    }

    if (waitForThreads) {
      for (final AsyncThread thread : threadPool.values()) {
        try {
          thread.join(1_000L);
        } catch (final InterruptedException e) {
          log.warn("Interrupted while waiting on AsyncThread {} to join, will "
                       + "skip waiting for any remaining threads to join and "
                       + "proceed with the shutdown", thread.getName());
          return;
        }
      }
    }
  }

}
