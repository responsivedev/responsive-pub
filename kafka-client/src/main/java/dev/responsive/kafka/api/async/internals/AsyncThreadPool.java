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
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool implements Closeable {

  private final Logger log;

  // TODO: start up new threads when existing ones die to maintain the target size
  private final int threadPoolSize;
  private final Map<String, AsyncThread> threadPool;

  public AsyncThreadPool(
      final int threadPoolSize,
      final ProcessorContext<?, ?> originalContext,
      final String streamThreadIndex,
      final ProcessingQueue<?, ?> processableRecords,
      final FinalizingQueue<?, ?> finalizableRecords
      ) {
    this.log = new LogContext(String.format(
        "async-threadpool [StreamThread-%s]", streamThreadIndex)
    ).logger(AsyncThreadPool.class);

    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);

    for (int i = 0; i < threadPoolSize; ++i) {
      final String name = threadName(streamThreadIndex, i);
      final AsyncThread thread = new AsyncThread(
          name,
          originalContext,
          processableRecords,
          finalizableRecords
      );

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

  private static String threadName(
      final String streamThreadIndex,
      final int asyncThreadIndex
  ) {
    return String.format("AsyncThread-%s-%d", streamThreadIndex, asyncThreadIndex);
  }

  @Override
  public void close() throws IOException {
    for (final AsyncThread thread : threadPool.values()) {
      thread.close();

      // TODO: use timeouts and retries in blocking queue to avoid need for interruption
      thread.interrupt();
    }

    int joinedThreads = 0;
    for (final AsyncThread thread : threadPool.values()) {
      // TODO: make this timeout configurable, or better yet determine it based on the
      //  timeout that the user passed into #close, since the async thread pool should
      //  not be shut down unless the app itself is too
      // TODO: or better yet...is it safe to just make them daemon threads and let them
      //  die on their own? Or register a shutdown handler and do this offline -- since
      //  we have to abandon the current pending progress anyway, it should be fine to
      //  complete the cleanup asynchronously as well
      try {
        thread.join(5_000L);
        ++joinedThreads;
      } catch (final InterruptedException e) {
        // If we received an interruption ourselves, we should prioritize exiting over joining
        log.warn("Interrupted while waiting on async threads to shut down, {} out of {} threads "
                     + "were able to join in time", joinedThreads, threadPoolSize);
      }
    }
  }
}
