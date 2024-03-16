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

package dev.responsive.kafka.api.async.internals.stores;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class AsyncProcessorFlushers {

  private final Map<String, AsyncStoreFlushListenerSuppliers> streamThreadToListenerSuppliers =
      new ConcurrentHashMap<>();

      public static class AsyncStoreFlushListenerSuppliers {

      }

  /**
   * An intermediate step towards narrowing down the specific AsyncFlushListener
   * that corresponds to a given physical AsyncProcessor instance.
   * Unfortunately the store builders are all constructed from the main application,
   * not the StreamThread we need to issue the flush call on, and these builders
   * will build the actual StateStore layers (including this) as well as the
   * AsyncProcessor instance when the task is first created, at which point we
   * don't have access to all the information we need to actually link them
   * together (specifically, the partition). We have to wait until #init is
   * called on both the AsyncProcessor and the StateStore so we can access the
   * partition info, and then together with the StreamThread name and store name,
   * can finally determine which AsyncProcessor we should hook up to and flush
   * when a commit occurs.
   * <p>
   * Threading notes:
   * -only ever accessed by StreamThreads
   * -should only ever be accessed by the same StreamThread
   * -one per processor per StreamThread per state store
   */
  public static class AsyncFlushListenerSupplier {

    private final Logger log;
    private final Map<Integer, AsyncFlushListener> partitionToListener;

    public AsyncFlushListenerSupplier(
        final String streamThreadName,
        final String storeName,
        final Map<Integer, AsyncFlushListener> partitionToListener
    ) {
      this.log = new LogContext(String.format(
          "stream-thread [%s] %s: ", streamThreadName, storeName
      )).logger(AsyncFlushListenerSupplier.class);
      this.partitionToListener = partitionToListener;
    }

    public AsyncFlushListener getListenerForPartition(final int partition) {
      final AsyncFlushListener listener = partitionToListener.remove(partition);
      if (listener == null) {
        log.error("No flush listeners were found for partition {}", partition);
        throw new IllegalStateException("Unable to locate the listener for "
            + "partition " + partition);
      }

      return listener;
    }
}

@FunctionalInterface
public interface AsyncFlushListener {

  /**
   * A simple runnable that, when executed, will flush all async buffers
   * and perform any output work to finalize pending events (such as issuing
   * their delayed forwards and writes) for a given AsyncProcessor.
   * <p>
   * This method will block until all in-flight events have been completed.
   * This may include simply waiting on the AsyncThreadPool to finish
   * processing all scheduled events and return them to the StreamThread,
   * or actively processing the output records of those records, or a mix
   * of both.
   * <p>
   * When a StreamThread creates a newly-assigned task and builds a new
   * AsyncProcessor instance in the topology, that processor will register
   * a new AsyncFlushListener with the processor's AsyncStoreBuilder (which
   * is shared by all StreamThreads). The StreamThread will then build any
   * state stores in the processor, at which time the AsyncStoreBuilder will
   * pass the AsyncFlushListener that corresponds to the current StreamThread,
   * <p>
   * Threading notes:
   * -to be executed by/on the StreamThread only
   * -one instance per physical AsyncProcessor
   *    (ie store per processor per partition per StreamThread)
   *    Important: in case of a zombie, there will two AsyncProcessors that
   *    correspond to the same logical AsyncProcessor and to the same
   *    task/partition. In this case, there will also be two AsyncFlushListeners,
   *    one for each of the two AsyncProcessors owned by different threads (the
   *    zombie and the "real" thread. It's important that an AsyncFlushListener
   *    always point to the same AsyncProcessor it was created for, and should
   *    not be updated/replaced when a zombie is fenced.
   *    This is why they are "per StreamThread" in addition to the rest
   * -while they are technically created for each individual state store
   *  within a processor, in the case of multiple state stores they will
   *  just end up pointing to the same AsyncProcessor and it will be flushed
   *  by each of the stores. Since flushing an emptied AsyncProcessor is a
   *  no-op, this is fine
   */
  void flushBuffers();
}
}
