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

import static dev.responsive.kafka.internal.utils.Utils.extractStreamThreadIndex;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO:
//  1) add an equivalent form for FixedKeyProcessorSupplier and the like
//  2) share single thread pool across all async processors per StreamThread
//  3) make thread pool size configurable
//  6) Should we reuse the same async context or is it ok to construct a new wrapper each #init?
//  7) Need to somehow associate per-record metadata with each input record AND provide
//     thread-safe access to the AsyncProcessorContext (perhaps via a wrapper that
//     designates a single, separate instance of the underlying context for each thread
//     (in the thread pool as well as the StreamThread itself)
public class AsyncProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

  private static final int THREAD_POOL_SIZE = 10;

  private Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

  private final Processor<KIn, VIn, KOut, VOut> userProcessor;
  private final Map<String, AsyncStoreBuilder<?>> connectedStores;

  // Owned and solely accessed by this StreamThread
  private final SchedulingQueue<KIn, VIn> schedulingQueue = new SchedulingQueue<>();

  private AsyncProcessorContext<KOut, VOut> asyncContext;
  private AsyncThreadPool<KIn, VIn, KOut, VOut> threadPool;

  // Note: the constructor will be called from the main application thread (ie the
  // one that creates/starts the KafkaStreams object) so we have to delay the creation
  // of most objects until #init since (a) that will be invoked by the actual
  // StreamThread processing this, and (b) we need the context supplied to init for
  // some of the setup
  public AsyncProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final Map<String, AsyncStoreBuilder<?>> connectedStores
  ) {
    this.userProcessor = userProcessor;
    this.connectedStores = connectedStores;
  }

  @Override
  public void init(final ProcessorContext<KOut, VOut> context) {
    this.asyncContext = new AsyncProcessorContext<>(context);
    this.log =
        new LogContext(String.format("async-processor [%s-%d]",
                      asyncContext.currentProcessorName(), asyncContext.partition())
        ).logger(AsyncProcessor.class);

    userProcessor.init(asyncContext);
    verifyConnectedStateStores();

    this.threadPool = new AsyncThreadPool<>(
        THREAD_POOL_SIZE, extractStreamThreadIndex(Thread.currentThread().getName())
    );
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    schedulingQueue.offer(record);

    // evaluate newly completed pending records and update queue/metadata

    Record<KIn, VIn> nextProcessableRecord = schedulingQueue.poll();
    while (nextProcessableRecord != null) {

      nextProcessableRecord = schedulingQueue.poll();
    }
    // check queue for all available records ready for processing
    // pass all records ready for processing to thread pool
  }

  @Override
  public void close() {
    userProcessor.close();
  }

  /**
   * Block on all pending records to be scheduled, executed, and fully complete processing through
   * the topology, as well as all state store operations to be applied. Called at the beginning of
   * each commit, similar to #flushCache
   */
  public void awaitCompletion() {
    // Check queue for newly processable records and hand them off to thread pool
    // Wait for pending records to be completed OR new blocked records to become processable
    // Repeat until input record queue is empty and all pending processing has finished
  }

  private void verifyConnectedStateStores() {
    final Map<String, AsyncKeyValueStore<?, ?>> allAsyncStores = asyncContext.getAllAsyncStores();
    if (allAsyncStores.size() != connectedStores.size()) {
      log.error("Number of connected store names is not equal to the number of stores retrieved "
                    + "via ProcessorContext#getStateStore during initialization. Make sure to pass "
                    + "all state stores used by this processor to the AsyncProcessorSupplier, and "
                    + "they are (all) initialized during the Processor#init call before actual "
                    + "processing begins. Found {} connected store names and {} actual stores used",
                connectedStores.size(), allAsyncStores.keySet().size());
      throw new IllegalStateException("Number of actual stores initialized by this processor does"
                                          + "not match the number of connected store names that were provided to the "
                                          + "AsyncProcessorSupplier");
    } else if (!connectedStores.keySet().containsAll(allAsyncStores.keySet())) {
      log.error("The list of connected store names was not identical to the set of store names "
                    + "that were used to access state stores via the ProcessorContext#getStateStore "
                    + "method during initialization. Double check the list of store names that are "
                    + "being passed in to the AsyncProcessorSupplier, and make sure it aligns with "
                    + "the actual store names being used by the processor itself. "
                    + "Got connectedStoreNames=[{}] and actualStoreNames=[{}]",
                connectedStores.keySet(), allAsyncStores.keySet());
      throw new IllegalStateException("The names of actual stores initialized by this processor do"
                                          + "not match the names of connected stores that were "
                                          + "provided to the AsyncProcessorSupplier");
    }
  }
}
