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

import dev.responsive.kafka.api.async.AsyncProcessorSupplier;
import dev.responsive.kafka.api.async.internals.contexts.AsyncContextRouter;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.StreamThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.ProcessingQueue;
import dev.responsive.kafka.api.async.internals.queues.SchedulingQueue;
import dev.responsive.kafka.api.async.internals.records.AsyncEvent;
import dev.responsive.kafka.api.async.internals.records.ScheduleableRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.slf4j.Logger;

// TODO:
//  1) add an equivalent form for FixedKeyProcessorSupplier and the like
//  2) share single thread pool across all async processors per StreamThread
//  3) make thread pool size configurable
//  6) Should we reuse the same async context or is it ok to construct a new wrapper each #init?
//  7) Need to somehow associate per-record metadata with each input record AND provide
//     thread-safe access to the AsyncProcessorContext (perhaps via a wrapper that
//     designates a single, separate instance of the underlying context for each thread
//     (in the thread pool as well as the StreamThread itself)
/**
 * Threading notes:
 * -Is exclusively owned and accessed by the StreamThread
 * -Coordinates the handoff of records between the StreamThread and AyncThreads
 */
public class AsyncProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

  private static final int THREAD_POOL_SIZE = 10;

  private final Processor<KIn, VIn, KOut, VOut> userProcessor;
  private final Map<String, AsyncStoreBuilder<?>> connectedStores;

  // Owned and solely accessed by this StreamThread, stashes waiting events that are blocked
  // on previous events with the same key that are still in flight
  private final SchedulingQueue<KIn, VIn> schedulingQueue = new SchedulingQueue<>();
  // Owned and solely accessed by this StreamThread, simply keeps track of the events that
  // are currently "in flight" which includes all events that were created by passing an
  // input record into the #process method of this AsyncProcessor, but have not yet reached
  // the DONE state and completed all execution of input and output records.
  // The StreamThread must wait until this set is empty before proceeding with an offset commit
  private final Set<AsyncEvent<KIn, VIn>> eventsInFlight = new HashSet<>();

  // Everything below this line is effectively final and just has to be initialized in #init
  // TODO: extract into class that allows us to make these final, eg an InitializedAsyncProcessor
  //  that doesn't get created until #init is invoked on this processor
  private String logPrefix;
  private Logger log;

  private String asyncProcessorName;
  private TaskId taskId;
  private AsyncThreadPool threadPool;
  private ProcessingQueue<KIn, VIn> processableRecords;
  private FinalizingQueue<KIn, VIn> finalizableRecords;

  // the context passed to us in init, ie the one that is used by Streams everywhere else
  private InternalProcessorContext<KOut, VOut> originalContext;
  // the async context owned by the StreamThread that executed this processor's #init
  // this wraps the originalContext but saves metadata for a "point in time" view
  private StreamThreadProcessorContext<KOut, VOut> streamThreadAsyncContext;

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
    this.taskId = context.taskId();
    this.originalContext = (InternalProcessorContext<KOut, VOut>) context;

    final String streamThreadName = extractStreamThreadIndex(Thread.currentThread().getName());
    this.asyncProcessorName = originalContext.currentNode().name();
    this.logPrefix = String.format("[%s-%d] ",
                                   originalContext.currentNode().name(), taskId.partition()
    );
    this.log = new LogContext(logPrefix).logger(AsyncProcessor.class);

    this.processableRecords = new ProcessingQueue<>(asyncProcessorName, taskId.partition());
    this.finalizableRecords = new FinalizingQueue<>(asyncProcessorName, taskId.partition());

    this.threadPool = new AsyncThreadPool(
        THREAD_POOL_SIZE,
        context,
        streamThreadName,
        processableRecords,
        finalizableRecords
    );

    final Map<String, AsyncThreadProcessorContext<KOut, VOut>> asyncThreadToContext =
        asyncThreadToContext(threadPool);

    this.streamThreadAsyncContext =
        new StreamThreadProcessorContext<>(originalContext, asyncThreadToContext);


    // Wrap the context handed to the user in the async router, to ensure that
    // subsequent calls to processor context APIs made within the user's #process
    // implementation will be routed to the specific async context corresponding
    // to the AsyncThread where that #process is currently being executed
    //  a) user calls are delegated to the context owned by the current thread
    //  b) async calls can be intercepted (ie forwards and StateStore reads/writes)
    final AsyncContextRouter<KOut, VOut> userContext =
        new AsyncContextRouter<>(asyncThreadToContext, streamThreadAsyncContext);

    userProcessor.init(userContext);

    final Map<String, AsyncKeyValueStore<?, ?>> accessedStores =
        streamThreadAsyncContext.getAllAsyncStores();

    verifyConnectedStateStores(accessedStores, connectedStores, log);
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    final AsyncEvent<KIn, VIn> newEvent = new AsyncEvent<>(
        asyncProcessorName,
        record,
        originalContext.recordContext(),
        () -> userProcessor.process(record)
    );
    eventsInFlight.add(newEvent);
    schedulingQueue.put(record, originalContext.recordContext());

    executeAvailableEvents();
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
    while (!isCompleted()) {

      drainSchedulingQueue();
      drainFinalizingQueue();

      // TODO: use a Condition to avoid busy waiting
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        throw new StreamsException("Interrupted while waiting for async completion", taskId);
      }
    }
  }

  private Map<String, AsyncThreadProcessorContext<KOut, VOut>> asyncThreadToContext(
      final AsyncThreadPool threadPool
  ) {
    final Set<String> asyncThreadNames = threadPool.asyncThreadNames();

    final Map<String, AsyncThreadProcessorContext<KOut, VOut>> asyncThreadToContext =
        new HashMap<>(asyncThreadNames.size());

    for (final String asyncThread : asyncThreadNames) {
      asyncThreadToContext.put(
          asyncThread,
          threadPool.asyncContextForThread(asyncThread)
      );
    }

    return asyncThreadToContext;
  }

  /**
   * Does a single, non-blocking pass over all queues to pull any events that are
   * currently available and ready to pick up to transition to the next stage in
   * the async event lifecycle. See {@link AsyncEvent.State} for more details on
   * the lifecycle states and requirements for transition.
   * <p>
   * While this method will execute all events that are returned from the queues
   * when polled, it does not attempt to fully drain the queues and will not
   * re-check the queues. Any events that become unblocked or are added to a
   * given queue while processing the other queues is not guaranteed to be
   * executed in this method invocation.
   * The queues are checked in an order that maximizes overall throughput, and
   * prioritizes moving events through the async processing pipeline over
   * maximizing the number of events we can get through in each call
   */
  private void executeAvailableEvents() {
    // Start by going through the events waiting to be finalized and finish executing their
    // outputs, if any, so we can mark them complete and potentially free up blocked events
    // waiting to be scheduled.

    // Then we check the scheduling queue and hand everything that is able to be processed
    // off to the processing queue for the AsyncThread to continue from here
    drainSchedulingQueue();
  }

  private void drainSchedulingQueue() {
    while (schedulingQueue.hasProcessableRecord()) {
      final AsyncEvent<KIn, VIn> processableEvent = schedulingQueue.poll();
      processableRecords.scheduleForProcessing(processableEvent);
    }
  }

  private void drainFinalizingQueue() {
    while (!finalizableRecords.isEmpty()) {
      final AsyncEvent<KIn, VIn> nextFinalizableEvent = finalizableRecords.nextFinalizableEvent();
      streamThreadAsyncContext.prepareToFinalizeEvent(nextFinalizableEvent.recordContext());

      while (!nextFinalizableEvent.isDone()) {
        streamThreadAsyncContext.executeDelayedWrite(nextFinalizableEvent.nextWrite());

        streamThreadAsyncContext.executeDelayedForward(nextFinalizableEvent.nextForward());
      }
    }
  }

  /**
   * @return true iff all records have been fully processed from start to finish
   */
  private boolean isCompleted() {
    return finalizableRecords.isEmpty();
  }

  /**
   * Verify that all the stores accessed by the user via {@link ProcessorContext#getStateStore(String)}
   * during their processor's #init method were connected to the processor following
   * the appropriate procedure for async processors. For more details, see the
   * instructions in the javadocs for {@link AsyncProcessorSupplier}.
   */
  private static void verifyConnectedStateStores(
      final Map<String, AsyncKeyValueStore<?, ?>> accessedStores,
      final Map<String, AsyncStoreBuilder<?>> connectedStores,
      final Logger log
  ) {
    if (accessedStores.size() != connectedStores.size()) {
      log.error("Number of connected store names is not equal to the number of stores retrieved "
                    + "via ProcessorContext#getStateStore during initialization. Make sure to pass "
                    + "all state stores used by this processor to the AsyncProcessorSupplier, and "
                    + "they are (all) initialized during the Processor#init call before actual "
                    + "processing begins. Found {} connected store names and {} actual stores used",
                connectedStores.size(), accessedStores.keySet().size());
      throw new IllegalStateException("Number of actual stores initialized by this processor does"
                                          + "not match the number of connected store names that were provided to the "
                                          + "AsyncProcessorSupplier");
    } else if (!connectedStores.keySet().containsAll(accessedStores.keySet())) {
      log.error("The list of connected store names was not identical to the set of store names "
                    + "that were used to access state stores via the ProcessorContext#getStateStore "
                    + "method during initialization. Double check the list of store names that are "
                    + "being passed in to the AsyncProcessorSupplier, and make sure it aligns with "
                    + "the actual store names being used by the processor itself. "
                    + "Got connectedStoreNames=[{}] and actualStoreNames=[{}]",
                connectedStores.keySet(), accessedStores.keySet());
      throw new IllegalStateException("The names of actual stores initialized by this processor do"
                                          + "not match the names of connected stores that were "
                                          + "provided to the AsyncProcessorSupplier");
    }
  }
}
