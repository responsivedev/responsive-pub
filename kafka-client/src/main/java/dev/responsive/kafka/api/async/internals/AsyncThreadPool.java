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

import dev.responsive.kafka.api.async.internals.contexts.AsyncProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.api.ProcessorContext;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool implements Closeable {

  // TODO: start up new threads when existing ones die to maintain the target size
  private final int threadPoolSize;
  private final Map<String, AsyncThread> threadPool;
  private final Map<String, AsyncThreadProcessorContext<?, ?>> threadToContext;

  public AsyncThreadPool(
      final int threadPoolSize,
      final ProcessorContext<?, ?> originalContext,
      final String streamThreadIndex,
      final ProcessingQueue<?, ?> processableRecords
  ) {
    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);
    this.threadToContext = new HashMap<>(threadPoolSize);

    for (int i = 0; i < threadPoolSize; ++i) {
      final String name = threadName(streamThreadIndex, i);
      final AsyncThread thread = new AsyncThread(
          name,
          originalContext,
          processableRecords,
          forwardableRecords,
          writeableRecords
      );

      threadPool.put(name, thread);
      threadToContext.put(name, thread.context());
    }
    for (final AsyncThread thread : threadPool.values()) {
      thread.start();
    }
  }

  public Set<String> asyncThreadNames() {
    return threadPool.keySet();
  }

  public AsyncThreadProcessorContext<?, ?> asyncContextForThread(final String threadName) {
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
    // TODO: wait for threads to join...or is it safe to just make them daemon threads?
  }
}
