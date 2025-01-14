/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
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
  private final Map<TaskId, List<ProcessorFlusher>> taskToAsyncProcessorFlushers = new HashMap<>();

  public AsyncThreadPoolRegistration(
      final AsyncThreadPool threadPool
  ) {
    this.threadPool = threadPool;
  }

  public AsyncThreadPool threadPool() {
    return threadPool;
  }

  // Called during processor initialization, which is done in topological order by Streams
  public void registerAsyncProcessor(final TaskId id, final ProcessorFlusher processorFlusher) {
    taskToAsyncProcessorFlushers
        .computeIfAbsent(id, (n) -> new ArrayList<>())
        .add(processorFlusher);
  }

  public void unregisterAsyncProcessor(final AsyncProcessorId id) {
    taskToAsyncProcessorFlushers.remove(id.taskId);
    threadPool.removeProcessor(id);
  }

  public void flushAsyncEventsForTask(final TaskId task) {
    final var flushers = taskToAsyncProcessorFlushers.get(task);
    if (flushers == null) {
      throw new IllegalStateException("Could not find task " + task + " to flush");
    }

    while (true) {
      boolean recordsFlushed = false;

      for (final var flusher : flushers) {

        // flusher returns true if any records were still buffering and had to be flushed
        if (flusher.flush()) {
          recordsFlushed = true;
        }

      }

      if (!recordsFlushed) {
        break;
      }
    }
  }

  public void flushAllAsyncEvents() {
    // TODO: this can be optimized by executing the tasks in parallel (while respecting
    //  the iteration order of flushes within a task which are topologically sorted)
    taskToAsyncProcessorFlushers.keySet().forEach(this::flushAsyncEventsForTask);
  }

  public void close() {
    threadPool.shutdown();
  }

  @FunctionalInterface
  public interface ProcessorFlusher {
    boolean flush();
  }
}
