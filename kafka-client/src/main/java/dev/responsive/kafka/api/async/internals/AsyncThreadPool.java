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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Coordinates communication between the StreamThread and execution threads, as
 * well as protecting and managing shared objects and accesses
 */
public class AsyncThreadPool<KIn, VIn, KOut, VOut> implements Closeable {

  private

  private final int threadPoolSize;
  private final Map<String, AsyncThread> threadPool;

  private final Queue<Record<KIn, VIn>> processableRecords;

  public AsyncThreadPool(final int threadPoolSize, final String streamThreadIndex) {
    this.threadPoolSize = threadPoolSize;
    this.threadPool = new HashMap<>(threadPoolSize);

    for (int i = 0; i < threadPoolSize; ++i) {
      final String name = threadName(streamThreadIndex, i);
      final AsyncThread thread = new AsyncThread(name);
      threadPool.put(name, thread);
    }
    for (final AsyncThread thread : threadPool.values()) {
      thread.start();
    }
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
      thread.stop();
    }
  }
}
