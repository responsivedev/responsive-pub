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
import dev.responsive.kafka.api.async.internals.queues.ForwardingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.queues.WritingQueue;
import dev.responsive.kafka.api.async.internals.records.AsyncEvent;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;

public class AsyncThread extends Thread implements Closeable {

  private final Logger log;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  // TODO: once we decouple the thread pool from the processor instance, these
  //  will need to become a map from processor to context/queue as there should be
  //  an async context per processor instance
  private final AsyncThreadProcessorContext<?, ?> asyncContext;
  private final ProcessingQueue<?, ?> processingQueue;
  private final FinalizingQueue<?, ?> finalizingQueue;

  public AsyncThread(
      final String name,
      final ProcessorContext<?, ?> context,
      final ProcessingQueue<?, ?> processingQueue,
      final FinalizingQueue<?, ?> finalizingQueue
  ) {
    super(name);
    this.asyncContext = new AsyncThreadProcessorContext<>(context, forwardingQueue);
    this.processingQueue = processingQueue;
    this.finalizingQueue = finalizingQueue;
    this.log = new LogContext(String.format("async-thread [%s] ", name)).logger(AsyncThread.class);
  }

  // TODO: once thread pool and processor are decoupled, this should look up the
  //  context in the map by processor node name
  public AsyncThreadProcessorContext<?, ?> context() {
    return asyncContext;
  }

  public <KIn, VIn> AsyncEvent<KIn, VIn> currentAsyncEvent() {
    return asyncContext.currentAsyncEvent();
  }

  @Override
  public void run() {
    try {
      while (!shutdownRequested.getOpaque()) {
        final AsyncEvent<?, ?> nextEvent = processingQueue.nextProcessableEvent();
        asyncProcess(nextEvent);
        finalizingQueue.scheduleForFinalization(nextEvent);
      }
    } catch (final Exception e) {
      if (shutdownRequested.getOpaque()) {
        log.info("An exception was thrown during shutdown, possibly as part of the shutdown", e);
      } else {
        log.error("Shutting down early due to exception thrown during async processing", e);
      }
    }
  }

  private <KIn, VIn> void asyncProcess(final AsyncEvent<?, ?> event) {
    asyncContext.prepareForAsyncProcess(event);
    event.processInputRecord().run();
  }

  @Override
  public void close() throws IOException {
    shutdownRequested.setOpaque(true);
  }

  /**
   * A reference to the {@link AsyncEvent} that is currently being processed
   * This class is used to update the current AsyncEvent in any async state stores,
   * ensuring the
   */
  public static class CurrentAsyncEvent {
    private AsyncEvent<?, ?> currentAsyncEvent;

    public AsyncEvent<?, ?> currentAsyncEvent() {
      return currentAsyncEvent;
    }

    public void updateCurrentAsyncEvent(final AsyncEvent<?, ?> asyncEvent) {
      this.currentAsyncEvent = asyncEvent;
    }
  }
}
