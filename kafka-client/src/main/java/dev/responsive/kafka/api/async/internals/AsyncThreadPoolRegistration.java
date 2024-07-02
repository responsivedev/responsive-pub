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

import java.util.HashMap;
import java.util.Map;

public class AsyncThreadPoolRegistration {

  private final AsyncThreadPool threadPool;

  private final Map<AsyncProcessorId, Runnable> asyncProcessorsToFlush = new HashMap<>();

  public AsyncThreadPoolRegistration(
      final AsyncThreadPool threadPool
  ) {
    this.threadPool = threadPool;
  }

  public AsyncThreadPool threadPool() {
    return threadPool;
  }

  public void registerAsyncProcessor(final AsyncProcessorId id, final Runnable flushProcessor) {
    asyncProcessorsToFlush.put(id, flushProcessor);
  }

  public void unregisterAsyncProcessor(final AsyncProcessorId id) {
    asyncProcessorsToFlush.remove(id);
    threadPool.removeProcessor(id);
  }

  public void flush() {
    // TODO: this is non-ideal for stateless apps since we're flush each processor instance
    //  sequentially and might end up waiting for async results from one processor while there
    //  are others ready and waiting on other processors/partitions.
    //  We should loop over all processors and only wait/block when we really have nothing to do
    //  (doesn't matter for stateful apps since this method is a no-op due to flushing
    //  everything during #flushCache which comes earlier in the commit process)
    asyncProcessorsToFlush.values().forEach(Runnable::run);
  }

  public void close() {
    threadPool.shutdown();
  }
}
