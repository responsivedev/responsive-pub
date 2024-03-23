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

/**
 * A simple container for all the "stuff" that's owned by and used for a
 * specific {@link AsyncProcessor} instance, ie each async processor that's
 * created for a given partition and on a particular StreamThread
 * <p>
 * Threading notes:
 * -simple final/immutable container that is thread-safe, but does not
 *  guarantee thread safety for its contents (ie they should also be
 *  thread-safe)
 * -one per physical AsyncProcessor
 *  ie per partition per processor per StreamThread
 * -used by the AsyncThreads to keep track of everything that corresponds
 *  to a particular partition (and StreamThread) in one place
 */
public final class AsyncProcessorContainer {

  // Three fields that uniquely identify the AsyncProcessor instance
  private final String streamThreadName;
  private final int partition;
  private final String asyncProcessorName;

  // The actual data structures needed to asynchronously process this instance
  private final AsyncThreadProcessorContext<?, ?> asyncContext;
  private final ProcessingQueue<?, ?> processingQueue;
  private final FinalizingQueue<?, ?> finalizingQueue;

  public AsyncProcessorContainer(
      final String streamThreadName,
      final int partition,
      final String asyncProcessorName,
      final AsyncThreadProcessorContext<?, ?> asyncContext,
      final ProcessingQueue<?, ?> processingQueue,
      final FinalizingQueue<?, ?> finalizingQueue
  ) {
    this.streamThreadName = streamThreadName;
    this.partition = partition;
    this.asyncProcessorName = asyncProcessorName;
    this.asyncContext = asyncContext;
    this.processingQueue = processingQueue;
    this.finalizingQueue = finalizingQueue;
  }

  public String streamThreadName() {
    return streamThreadName;
  }

  public int partition() {
    return partition;
  }

  public String asyncProcessorName() {
    return asyncProcessorName;
  }

  public AsyncThreadProcessorContext<?, ?> asyncContext() {
    return asyncContext;
  }

  public ProcessingQueue<?, ?> processingQueue() {
    return processingQueue;
  }

  public FinalizingQueue<?, ?> finalizingQueue() {
    return finalizingQueue;
  }
}
