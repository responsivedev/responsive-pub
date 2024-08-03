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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.TaskId;

public class AsyncThreadPoolRegistration {

  private final AsyncThreadPool threadPool;

  // Processors are maintained in topological order for each partition/task
  private final Map<TaskId, List<Runnable>> taskToAsyncProcessorFlushers = new HashMap<>();

  public AsyncThreadPoolRegistration(
      final AsyncThreadPool threadPool
  ) {
    this.threadPool = threadPool;
  }

  public AsyncThreadPool threadPool() {
    return threadPool;
  }

  // Called during processor initialization, which is done in topological order by Streams
  public void registerAsyncProcessor(final TaskId id, final Runnable flushProcessor) {
    taskToAsyncProcessorFlushers
        .computeIfAbsent(id, (n) -> new ArrayList<>())
        .add(flushProcessor);
  }

  public void unregisterAsyncProcessor(final AsyncProcessorId id) {
    taskToAsyncProcessorFlushers.remove(id.taskId);
    threadPool.removeProcessor(id);
  }

  public void flushAllAsyncEvents() {
    // TODO: this can be optimized by executing the tasks in parallel (while respecting
    //  the iteration order of flushes within a task which are topologically sorted)
    taskToAsyncProcessorFlushers.values().forEach(flushers -> {
      // These must be executed in order
      for (final var flush : flushers) {
        flush.run();
      }
    });
  }

  public void close() {
    threadPool.shutdown();
  }
}
