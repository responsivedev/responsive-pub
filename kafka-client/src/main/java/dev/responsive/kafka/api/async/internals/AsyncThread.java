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

import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public class AsyncThread extends Thread implements Closeable {

  private boolean initialized = false;

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  // TODO: once we decouple the thread pool from the processor instance, these
  //  will need to become a map from processor to context/queue as there should be
  //  an async context per processor instance
  private final AsyncProcessorContext<?, ?> asyncContext;
  private final ProcessingQueue<?, ?> processableRecords;

  public AsyncThread(
      final String name,
      final ProcessorContext<?, ?> context,
      final ProcessingQueue<?, ?> processableRecords
  ) {
    super(name);
    this.asyncContext = new AsyncProcessorContext<>(context);
    this.processableRecords = processableRecords;
  }

  public void init(final AsyncProcessorContext<?, ?> asyncContext) {

  }

  // TODO: once thread pool and processor are decoupled, this should look up the
  //  context in the map by processor node name
  public AsyncProcessorContext<?, ?> context() {
    return asyncContext;
  }

  @Override
  public void run() {
    if (!initialized) {
      throw new IllegalStateException("Attempted to start up async thread before initialization");
    }

    while (!shutdownRequested.getOpaque()) {
      processableRecords.poll().process(asyncContext);
    }
  }

  @Override
  public void close() throws IOException {
    shutdownRequested.setOpaque(true);
  }
}
