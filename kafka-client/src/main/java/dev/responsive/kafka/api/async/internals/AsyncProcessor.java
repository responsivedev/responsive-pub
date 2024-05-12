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

import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_FLUSH_INTERVAL_MS_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadAsyncThreadPoolRegistry;

import dev.responsive.kafka.api.async.AsyncProcessorSupplier;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.StreamThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.events.DelayedForward;
import dev.responsive.kafka.api.async.internals.events.DelayedWrite;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.api.async.internals.queues.SchedulingQueue;
import dev.responsive.kafka.api.async.internals.stores.AbstractAsyncStoreBuilder;
import dev.responsive.kafka.api.async.internals.stores.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.internals.stores.StreamThreadFlushListeners.AsyncFlushListener;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.slf4j.Logger;

/**
 * Threading notes:
 * -Is exclusively owned and accessed by the StreamThread
 * -Coordinates the handoff of records between the StreamThread and AyncThreads
 * -The starting and ending point of all async events -- see {@link AsyncEvent}
 */
public class AsyncProcessor<KIn, VIn, KOut, VOut>
    implements Processor<KIn, VIn, KOut, VOut>, FixedKeyProcessor<KIn, VIn, VOut> {

  // Exactly one of these is non-null and the other is null
  private final Processor<KIn, VIn, KOut, VOut> userProcessor;
  private final FixedKeyProcessor<KIn, VIn, VOut> userFixedKeyProcessor;

  private final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders;

  // Tracks all pending events, ie from moment of creation to end of finalization/transition
  // to "DONE" state. Used to make sure all events are flushed during a commit
  private final Set<AsyncEvent> pendingEvents = new HashSet<>();


  // This is set at most once. When its set, the thread should immediately throw, and no longer
  // try to process further events for this processor. This minimizes the chance of producing
  // bad results, particularly with ALOS.
  private StreamsException fatalException = null;

  // Everything below this line is effectively final and just has to be initialized in #init //

  private String logPrefix;
  private Logger log;

  private String streamThreadName;
  private String asyncProcessorName;
  private TaskId taskId;

  private AsyncThreadPool threadPool;
  private SchedulingQueue<KIn> schedulingQueue;
  private FinalizingQueue finalizingQueue;

  private Cancellable punctuator;

  // the context passed to us in init, ie the one created for this task and owned by Kafka Streams
  private ProcessingContext taskContext;

  // the async context owned by the StreamThread that is running this processor/task
  private StreamThreadProcessorContext<KOut, VOut> streamThreadContext;

  // the context we pass in to the user so it routes to the actual context based on calling thread
  private AsyncUserProcessorContext<KOut, VOut> userContext;
  private boolean hasProcessedSomething = false;

  public static <KIn, VIn, KOut, VOut> AsyncProcessor<KIn, VIn, KOut, VOut> createAsyncProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    return new AsyncProcessor<>(userProcessor, null, connectedStoreBuilders);
  }

  public static <KIn, VIn, VOut> AsyncProcessor<KIn, VIn, KIn, VOut> createAsyncFixedKeyProcessor(
      final FixedKeyProcessor<KIn, VIn, VOut> userProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    return new AsyncProcessor<>(null, userProcessor, connectedStoreBuilders);
  }

  // Note: the constructor will be called from the main application thread (ie the
  // one that creates/starts the KafkaStreams object) so we have to delay the creation
  // of most objects until #init since (a) that will be invoked by the actual
  // StreamThread processing this, and (b) we need the context supplied to init for
  // some of the setup
  private AsyncProcessor(
      final Processor<KIn, VIn, KOut, VOut> userProcessor,
      final FixedKeyProcessor<KIn, VIn, VOut> userFixedKeyProcessor,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStoreBuilders
  ) {
    this.userProcessor = userProcessor;
    this.userFixedKeyProcessor = userFixedKeyProcessor;
    this.connectedStoreBuilders = connectedStoreBuilders;

    if (userProcessor == null && userFixedKeyProcessor == null) {
      throw new IllegalStateException("Both the Processor and FixedKeyProcessor were null");
    } else if (userProcessor != null && userFixedKeyProcessor != null) {
      throw new IllegalStateException("Both the Processor and FixedKeyProcessor were non-null");
    }
  }

  @Override
  public void init(final ProcessorContext<KOut, VOut> context) {

    initFields((InternalProcessorContext<KOut, VOut>) context);

    userProcessor.init(userContext);

    completeInitialization();
  }

  // Note: we have to cast and suppress warnings in this version of #init but
  // not the other due to the KOut parameter being squashed into KIn in the
  // fixed-key version of the processor. However, we know this cast is safe,
  // since by definition KIn and KOut are the same type
  @SuppressWarnings("unchecked")
  @Override
  public void init(final FixedKeyProcessorContext<KIn, VOut> context) {

    initFields((InternalProcessorContext<KOut, VOut>) context);

    userFixedKeyProcessor.init((FixedKeyProcessorContext<KIn, VOut>) userContext);

    completeInitialization();
  }

  /**
   * Performs the first half of initialization by setting all the class fields
   * that have to wait for the context to be passed in to #init to be initialized.
   */
  private void initFields(
      final InternalProcessorContext<KOut, VOut> internalContext
  ) {
    this.taskContext = internalContext;

    this.streamThreadName = Thread.currentThread().getName();
    this.taskId = internalContext.taskId();
    this.asyncProcessorName = internalContext.currentNode().name();

    this.logPrefix = String.format(
        "stream-thread [%s] %s[%d] ",
        streamThreadName, asyncProcessorName, taskId.partition()
    );
    this.log = new LogContext(logPrefix).logger(AsyncProcessor.class);

    this.userContext = new AsyncUserProcessorContext<>(
        streamThreadName,
        taskContext,
        logPrefix
    );
    this.streamThreadContext = new StreamThreadProcessorContext<>(
        logPrefix,
        internalContext,
        userContext
    );
    userContext.setDelegateForStreamThread(streamThreadContext);

    final ResponsiveConfig configs = ResponsiveConfig.responsiveConfig(userContext.appConfigs());
    final long punctuationInterval = configs.getLong(ASYNC_FLUSH_INTERVAL_MS_CONFIG);
    final int maxEventsPerKey = configs.getInt(ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG);

    this.threadPool = getAsyncThreadPool(taskContext, streamThreadName);

    this.schedulingQueue = new SchedulingQueue<>(logPrefix, maxEventsPerKey);
    this.finalizingQueue = new FinalizingQueue(logPrefix, taskId.partition());

    this.punctuator = taskContext.schedule(
        Duration.ofMillis(punctuationInterval),
        PunctuationType.WALL_CLOCK_TIME,
        this::punctuate
    );
  }

  /**
   * Performs the second half of initialization after calling #init on the user's processor.
   * Verifies and registers any async stores that were created by the user accessing
   * connected stores via the {@link ProcessingContext#getStateStore(String)} API during #init
   */
  private void completeInitialization() {
    final Map<String, AsyncKeyValueStore<?, ?>> accessedStores =
        streamThreadContext.getAllAsyncStores();

    verifyConnectedStateStores(accessedStores, connectedStoreBuilders);

    registerFlushListenerForStoreBuilders(
        streamThreadName,
        taskId.partition(),
        connectedStoreBuilders.values(),
        this::flushAndAwaitPendingEvents
    );
  }

  void assertQueuesEmptyOnFirstProcess() {
    if (!hasProcessedSomething) {
      assertQueuesEmpty();
      hasProcessedSomething = true;
    }
  }

  void assertQueuesEmpty() {
    if (!schedulingQueue.isEmpty()) {
      log.error("the scheduling queue for {} was expected to be empty", taskId);
      throw new IllegalStateException("scheduling queue expected to be empty");
    }
    if (!threadPool.isEmpty(asyncProcessorName, taskId.partition())) {
      log.error("the thread pool for {} was expected to be empty", taskId);
      throw new IllegalStateException("thread pool expected to be empty");
    }
    if (!finalizingQueue.isEmpty()) {
      log.error("the finalizing queue for {} was expected to be empty", taskId);
      throw new IllegalStateException("finalizing queue expected to be empty");
    }
  }

  @Override
  public void process(final Record<KIn, VIn> record) {
    assertQueuesEmptyOnFirstProcess();

    final AsyncEvent newEvent = new AsyncEvent(
        logPrefix,
        record,
        asyncProcessorName,
        taskId.partition(),
        extractRecordContext(taskContext),
        taskContext.currentStreamTimeMs(),
        taskContext.currentSystemTimeMs(),
        () -> userProcessor.process(record)
    );

    processNewAsyncEvent(newEvent);
  }

  @Override
  public void process(final FixedKeyRecord<KIn, VIn> record) {
    assertQueuesEmptyOnFirstProcess();

    final AsyncEvent newEvent = new AsyncEvent(
        logPrefix,
        record,
        asyncProcessorName,
        taskId.partition(),
        extractRecordContext(taskContext),
        taskContext.currentStreamTimeMs(),
        taskContext.currentSystemTimeMs(),
        () -> userFixedKeyProcessor.process(record)
    );

    processNewAsyncEvent(newEvent);
  }

  private void processNewAsyncEvent(final AsyncEvent event) {
    if (fatalException != null) {
      log.error("process called when processor already hit fatal exception", fatalException);
      throw new IllegalStateException(
          "process called when already hit exception: " + fatalException);
    }

    try {
      pendingEvents.add(event);

      maybeBackOffEnqueuingNewEventWithKey(event.inputRecordKey());
      schedulingQueue.offer(event);

      executeAvailableEvents();

    } catch (final StreamsException streamsException) {
      fatalException = streamsException.taskId().isPresent()
          ? streamsException
          : new StreamsException(streamsException, taskId);
      throw fatalException;
    } catch (final Throwable t) {
      fatalException = new StreamsException(t, taskId);
      throw fatalException;
    }
  }

  @SuppressWarnings("unchecked")
  private ProcessorRecordContext extractRecordContext(final ProcessingContext context) {
    // Alternatively we could use the ProcessingContext#recordMetadata and then cast that
    // to a ProcessorRecordContext, but either way a cast somewhere is unavoidable
    return ((InternalProcessorContext<KOut, VOut>) context).recordContext();
  }

  @Override
  public void close() {

    if (!isCleared()) {
      // This doesn't necessarily indicate an issue; it just should only ever
      // happen if the task is closed dirty, but unfortunately we can't tell
      // from here whether that was the case. Log a warning here so that it's
      // possible to determine whether something went wrong or not by looking
      // at the complete logs for the task/thread
      log.warn("Closing async processor with {} in-flight events, this should only "
                   + "happen if the task was shut down dirty and not flushed/committed "
                   + "prior to being closed", pendingEvents.size());
    }

    punctuator.cancel();
    threadPool.removeProcessor(asyncProcessorName, taskId.partition());

    if (userProcessor != null) {
      userProcessor.close();
    } else {
      userFixedKeyProcessor.close();
    }
  }

  private static void registerFlushListenerForStoreBuilders(
      final String streamThreadName,
      final int partition,
      final Collection<AbstractAsyncStoreBuilder<?, ?, ?>> asyncStoreBuilders,
      final AsyncFlushListener flushPendingEvents
  ) {
    for (final AbstractAsyncStoreBuilder<?, ?, ?> builder : asyncStoreBuilders) {
      builder.registerFlushListenerWithAsyncStore(streamThreadName, partition, flushPendingEvents);
    }
  }

  /**
   * Block on all pending records to be scheduled, executed, and fully complete processing through
   * the topology, as well as all state store operations to be applied. Called at the beginning of
   * each commit, similar to #flushCache
   */
  public void flushAndAwaitPendingEvents() {
    if (fatalException != null) {
      // if there was a fatal exception, just throw right away so that we exit right
      // away and minimize the risk of causing further problems. Additionally, processing for
      // the key that hit the processing exception is blocked, so we don't want to go into the
      // loop below that tries to drain queues, as the queue for that key will never drain.
      log.error("exit flush early due to previous fatal exception", fatalException);
      throw fatalException;
    }

    try {
      // Make a (non-blocking) pass through the finalizing queue up front, to
      // free up any recently-processed events before we attempt to drain the
      // scheduling queue
      drainFinalizingQueue();

      while (!isCleared()) {
        // Start by scheduling all unblocked events to hand off any events that
        // were just unblocked by whatever we just finalized
        final int numScheduled = drainSchedulingQueue();

        // Need to finalize at least one event per iteration, otherwise there's no
        // point returning to the scheduling queue since nothing new was unblocked
        final int numFinalized = finalizeAtLeastOneEvent();
        log.trace("Scheduled {} events and finalized {} events",
                  numScheduled, numFinalized
        );
      }

      assertQueuesEmpty();

    } catch (final StreamsException streamsException) {
      fatalException = streamsException.taskId().isPresent()
        ? streamsException
        : new StreamsException(streamsException, taskId);
      throw fatalException;
    } catch (final Throwable t) {
      fatalException = new StreamsException(t, taskId);
      throw fatalException;
    }
  }

  /**
   * Check the AsyncThreadPool for any uncaught exceptions that arose while processing
   * events or other fatal errors in the AsyncThreadPool itself.
   * Note: since it's important to fail as quickly as possible in the event of a
   * fatal exception, we don't limit the scope of this check to only errors
   * tied to events that belong to this specific AsyncProcessor or task.
   */
  private void checkUncaughtExceptionsInAsyncThreadPool() throws Throwable {
    final Optional<Throwable> fatal = threadPool.checkUncaughtExceptions(
        asyncProcessorName, taskId.partition()
    );
    
    if (fatal.isPresent()) {
      throw fatal.get();
    }
  }

  /**
   * Blocking API that guarantees at least one event has been finalized.
   * <p>
   * Drains the finalizing queue to complete any/all events that were already
   * processed and waiting to be finalized. If no events are ready for
   * finalization when this method is called, it will block until the next
   * one becomes available and will finalize that event.
   *
   * @return the number of events that were finalized
   */
  private int finalizeAtLeastOneEvent() throws Throwable {
    checkUncaughtExceptionsInAsyncThreadPool();

    final int numFinalized = drainFinalizingQueue();
    if (numFinalized > 0) {
      return numFinalized;
    }

    while (true) {
      // There is a low chance that an event fails in such a way that it's never placed on
      // the finalizing queue (this _should_ never happen unless there is a bug, which is
      // why it's "low-chance"). To account for this possibility we block on finalizable
      // events in a loop and apply a timeout to the wait so we can check for fatal
      // errors outside of the FinalizingQueue
      final AsyncEvent finalizableEvent = finalizingQueue.waitForNextFinalizableEvent(
          100,
          TimeUnit.MILLISECONDS
      );

      if (finalizableEvent != null) {
        completePendingEvent(finalizableEvent);
        return 1;
      }

      checkUncaughtExceptionsInAsyncThreadPool();
    }
  }

  /**
   * Executes async events that are in-flight until the SchedulingQueue has
   * adequate space for new events.
   * <p>
   * Applies backpressure before proceeding with a new AsyncEvent ie adding it
   * to the SchedulingQueue. This method waits for new events to finish being
   * processed by the async threads and handed back to the StreamThread for
   * finalization, thus freeing up blocked record(s) that were waiting in the
   * SchedulingQueue and can be moved to the ProcessingQueue for processing.
   */
  private void maybeBackOffEnqueuingNewEventWithKey(final KIn key) throws Throwable {
    while (schedulingQueue.keyQueueIsFull(key)) {
      drainSchedulingQueue();

      if (schedulingQueue.keyQueueIsFull(key)) {
        // we may not actually have finalized an event with this key, but we
        // may as well return to the loop start so we can potentially schedule
        // a newly-unblocked event of a different key
        finalizeAtLeastOneEvent();
      }
    }
  }

  /**
   * Invoked by the punctuator for this processor
   */
  private void punctuate(final long timestamp) {
    try {
      log.debug("Flushing async events during punctuation at {}", timestamp);

      executeAvailableEvents();
    } catch (final StreamsException streamsException) {
      fatalException = streamsException.taskId().isPresent()
          ? streamsException
          : new StreamsException(streamsException, taskId);
      throw fatalException;
    } catch (final Throwable t) {
      fatalException = new StreamsException(t, taskId);
      throw fatalException;
    }
  }

  /**
   * Does a single pass over all queues to pull any events that are
   * currently available and ready to pick up to transition to the next stage in
   * the async event lifecycle. Does not wait for more events if the queues are empty,
   * but may still block when draining the scheduling queue until the processing queue
   * for the async threadpool goes below the cap on how many events it can have queued
   * up. See {@link AsyncEvent.State} for more details on the event lifecycle and
   * requirements for state transitions.
   * <p>
   * While this method will execute all events that are returned from the queues
   * when polled, it does not attempt to fully drain the queues and will not
   * re-check the queues. Any events that become unblocked or are added to a
   * given queue while processing the other queues are not guaranteed to be
   * executed in this method invocation.
   * The queues are checked in an order that maximizes overall throughput, and
   * prioritizes moving events through the async processing pipeline over
   * maximizing the number of events we can get through in each call
   * <p>
   * Note: all per-event logging should be at TRACE to avoid spam
   */
  private void executeAvailableEvents() throws Throwable {
    // Start by going through the events waiting to be finalized and finish executing their
    // outputs, if any, so we can mark them complete and potentially free up blocked events
    // waiting to be scheduled.
    final int numFinalized = drainFinalizingQueue();
    log.trace("Finalized {} events", numFinalized);

    // Then we check the scheduling queue and hand everything that is able to be processed
    // off to the processing queue for the AsyncThread to continue from here
    final int numScheduled = drainSchedulingQueue();
    log.trace("Scheduled {} events", numScheduled);
  }

  /**
   * Polls all the available records from the {@link SchedulingQueue}
   * without waiting for any blocked events to become schedulable.
   * Makes a single pass and schedules only the current set of
   * schedulable events.
   * May block if the processing queue is at capacity.
   * <p>
   * There may be blocked events still waiting in the scheduling queue
   * after each invocation of this method, so the caller should make
   * sure to test whether the scheduling queue is empty after this method
   * returns, if the goal is to schedule all pending events.
   *
   * @return the number of events that were scheduled
   */
  private int drainSchedulingQueue() {
    final List<AsyncEvent> eventsToSchedule = new LinkedList<>();

    while (schedulingQueue.hasProcessableRecord()) {
      final AsyncEvent processableEvent = schedulingQueue.poll();
      processableEvent.transitionToToProcess();
      eventsToSchedule.add(processableEvent);
    }
    final int numScheduled = eventsToSchedule.size();
    if (numScheduled > 0) {
      threadPool.scheduleForProcessing(
          asyncProcessorName,
          taskId.partition(),
          eventsToSchedule,
          finalizingQueue,
          taskContext,
          userContext
      );
    }

    return numScheduled;
  }

  /**
   * Polls all the available records from the {@link FinalizingQueue}
   * without waiting for any new events to arrive. Makes a single pass
   * and completes only the current set of finalizable events, which
   * means there will most likely be pending events still in flight
   * for this processor that will need to be finalized later.
   *
   * @return the number of events that were finalized
   */
  private int drainFinalizingQueue() throws Throwable {
    checkUncaughtExceptionsInAsyncThreadPool();

    int count = 0;
    while (!finalizingQueue.isEmpty()) {
      final AsyncEvent event = finalizingQueue.nextFinalizableEvent();
      completePendingEvent(event);
      ++count;
    }
    return count;
  }

  /**
   * Complete processing one pending event.
   * Accepts an event pulled from the {@link FinalizingQueue} and finalizes
   * it before marking the event as done, or throws an exception if the
   * event failed at any stage before or during finalization
   *
   * @throws Throwable if the event failed at any stage, eg during processing or finalization
   */
  @SuppressWarnings("try")
  private void completePendingEvent(final AsyncEvent finalizableEvent) throws Throwable {

    try (final var ignored = preFinalize(finalizableEvent)) {
      doFinalize(finalizableEvent);
      postFinalize(finalizableEvent);
    }
  }


  private StreamThreadProcessorContext.PreviousRecordContextAndNode preFinalize(
      final AsyncEvent event
  ) throws Throwable {

    if (!pendingEvents.contains(event)) {
      log.error("routed event from {} to the wrong processor for {}",
          event.partition(),
          taskId.toString());
      throw new IllegalStateException(String.format(
          "routed event from %d to the wrong processor for %s",
          event.partition(),
          taskId.toString()));
    }

    final var toClose = streamThreadContext.prepareToFinalizeEvent(event);

    final Optional<Throwable> processingException = event.processingException();
    if (processingException.isEmpty()) {
      event.transitionToFinalizing();
    } else {
      pendingEvents.remove(event);
      event.transitionToDone();
      throw processingException.get();
    }

    return toClose;
  }

  /**
   * Perform finalization of this event by processing output records,
   * ie executing forwards and writes that were intercepted from #process
   */
  private void doFinalize(final AsyncEvent event) {
    DelayedWrite<?, ?> nextDelayedWrite = event.nextWrite();
    DelayedForward<KOut, VOut> nextDelayedForward = event.nextForward();

    while (nextDelayedWrite != null || nextDelayedForward != null) {

      if (nextDelayedWrite != null) {
        streamThreadContext.executeDelayedWrite(nextDelayedWrite);
      }

      if (nextDelayedForward != null) {
        streamThreadContext.executeDelayedForward(nextDelayedForward);
      }

      nextDelayedWrite = event.nextWrite();
      nextDelayedForward = event.nextForward();
    }
  }

  /**
   * After finalization, the event can be transitioned to {@link AsyncEvent.State#DONE}
   * and cleared from the set of pending events, unblocking that key.
   */
  private void postFinalize(final AsyncEvent event) {
    event.transitionToDone();

    pendingEvents.remove(event);
    schedulingQueue.unblockKey(event.inputRecordKey());
  }

  /**
   * @return true iff all records have been fully processed from start to finish
   */
  private boolean isCleared() {
    return pendingEvents.isEmpty();
  }

  /**
   * Verify that all the stores accessed by the user via
   * {@link ProcessorContext#getStateStore(String)} during their processor's
   * #init method were connected to the processor following the appropriate
   * procedure for async processors. For more details, see the instructions
   * in the javadocs for {@link AsyncProcessorSupplier}.
   */
  private void verifyConnectedStateStores(
      final Map<String, AsyncKeyValueStore<?, ?>> accessedStores,
      final Map<String, AbstractAsyncStoreBuilder<?, ?, ?>> connectedStores
  ) {
    if (!accessedStores.keySet().equals(connectedStores.keySet())) {
      log.error(
          "Connected stores names not equal to the stores retrieved "
              + "via ProcessorContext#getStateStore during initialization. Make sure to pass "
              + "all state stores used by this processor to the AsyncProcessorSupplier, and "
              + "they are (all) initialized during the Processor#init call before actual "
              + "processing begins. Found {} connected store names and {} actual stores used",
          String.join(",", connectedStores.keySet()),
          String.join(", ", accessedStores.keySet()));
      throw new IllegalStateException(
          "Names of actual stores initialized by this processor does not "
              + "match the connected store names that were provided "
              + "to the AsyncProcessorSupplier");
    }
  }

  private static AsyncThreadPool getAsyncThreadPool(
      final ProcessingContext context,
      final String streamThreadName
  ) {
    try {
      final AsyncThreadPoolRegistry registry = loadAsyncThreadPoolRegistry(
          context.appConfigsWithPrefix(StreamsConfig.MAIN_CONSUMER_PREFIX)
      );
      return registry.asyncThreadPoolForStreamThread(streamThreadName);
    } catch (final Exception e) {
      throw new ConfigException(
          "Unable to locate async thread pool registry. Make sure to configure "
              + ResponsiveConfig.ASYNC_THREAD_POOL_SIZE_CONFIG, e);
    }
  }
}
