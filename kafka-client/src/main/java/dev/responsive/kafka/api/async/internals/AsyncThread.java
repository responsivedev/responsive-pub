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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public class AsyncThread extends Thread implements Closeable {

  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  // TODO: once we decouple the thread pool from the processor instance, this
  //  will need to become a map from processor to context as there should be
  //  an async context per processor instance
  private final AsyncProcessorContext<?, ?> asyncContext;


  public AsyncThread(
      final String name,
      final ProcessorContext<?, ?> context
  ) {
    super(name);
    this.asyncContext = new AsyncProcessorContext<Object, Object>(context);
  }

  @Override
  public void run() {
    while (!shutdownRequested.getOpaque()) {
      // TODO
    }
  }

  @Override
  public void close() throws IOException {
    shutdownRequested.setOpaque(true);
  }
}
