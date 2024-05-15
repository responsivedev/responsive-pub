package dev.responsive.kafka.api.async.internals;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

public class AsyncThreadPool {
  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final Logger log;

  private final ThreadPoolExecutor executor;
  private final Map<InFlightWorkKey, ConcurrentMap<AsyncEvent, InFlightEvent>> inFlight
      = new HashMap<>();
  private final Semaphore queueSemaphore;
  private final BlockingQueue<Runnable> processingQueue;

  // TODO: we don't really need to track this by processor/partition, technically an
  //  error anywhere is fatal for the StreamThread and all processors + all further
  //  processing should shut down ASAP to minimize ALOS overcounting
  private final Map<InFlightWorkKey, Throwable> fatalExceptions = new ConcurrentHashMap<>();

  private final AtomicInteger threadNameIndex = new AtomicInteger(0);

  public AsyncThreadPool(
      final String streamThreadName,
      final int threadPoolSize,
      final int maxQueuedEvents
  ) {
    final LogContext logPrefix
        = new LogContext(String.format("stream-thread [%s] ", streamThreadName));
    this.log = logPrefix.logger(AsyncThreadPool.class);
    this.processingQueue = new LinkedBlockingQueue<>();
    this.queueSemaphore = new Semaphore(maxQueuedEvents);

    this.executor = new ThreadPoolExecutor(
        threadPoolSize,
        maxQueuedEvents,
        Long.MAX_VALUE,
        TimeUnit.DAYS,
        processingQueue,
        r -> {
          final var t = new Thread(r);
          t.setDaemon(true);
          t.setName(generateAsyncThreadName(streamThreadName, threadNameIndex.getAndIncrement()));
          return t;
        }
    );
  }

  public boolean isEmpty(final String processorName, final int partition) {
    final var forTask = inFlight.get(InFlightWorkKey.of(processorName, partition));
    if (forTask == null) {
      log.debug("No in-flight map found for {}[{}]", processorName, partition);
      return true;
    }
    if (log.isTraceEnabled()) {
      log.trace("Found in-flight map for {}[{}]: {}",
          processorName,
          partition,
          forTask.keySet().stream().map(AsyncEvent::toString).collect(Collectors.joining(", "))
      );
    }
    return forTask.isEmpty();
  }

  public void removeProcessor(final String processorName, final int partition) {
    log.debug("Removing {}[{}] from async thread pool", processorName, partition);
    final var key = InFlightWorkKey.of(processorName, partition);
    final Map<AsyncEvent, InFlightEvent> inFlightForTask = inFlight.remove(key);


    if (inFlightForTask != null) {
      log.info("Cancelling {} pending records for {}[{}]",
               inFlightForTask.size(), processorName, partition);
      inFlightForTask.values().forEach(f -> f.future().cancel(true));
    }
  }

  /**
   * @return any uncaught exceptions encountered during processing of input records,
   *         or {@link Optional#empty()} if there are none
   */
  public Optional<Throwable> checkUncaughtExceptions(
      final String processorName,
      final int partition
  ) {
    return Optional.ofNullable(fatalExceptions.get(new InFlightWorkKey(processorName, partition)));

  }

  @VisibleForTesting
  Map<AsyncEvent, InFlightEvent> getInFlight(final String processorName, final int partition) {
    return inFlight.get(InFlightWorkKey.of(processorName, partition));
  }

  /**
   * @return the name for this AsyncThread, formatted by appending the async thread suffix
   *         based on a unique async-thread index i and the base name of the StreamThread
   *         with index n, ie
   *         AsyncThread.getName() --> {clientId}-StreamThread-{n}-AsyncThread-{i}
   */
  private static String generateAsyncThreadName(
      final String streamThreadName,
      final int asyncThreadIndex
  ) {
    return String.format("%s-%s-%d", streamThreadName, ASYNC_THREAD_NAME, asyncThreadIndex);
  }

  /**
   * Schedule a new event for processing. Must be "processable" ie all previous
   * same-key events have cleared.
   * <p>
   * This is a potentially blocking call, as it will wait until the processing queue has
   * enough space to accept a new event.
   */
  public <KOut, VOut> void scheduleForProcessing(
      final String processorName,
      final int partition,
      final List<AsyncEvent> events,
      final FinalizingQueue finalizingQueue,
      final ProcessingContext taskContext,
      final AsyncUserProcessorContext<KOut, VOut> asyncProcessorContext
  ) {
    final var inFlightKey = InFlightWorkKey.of(processorName, partition);
    final var inFlightForTask
        = inFlight.computeIfAbsent(inFlightKey, k -> new ConcurrentHashMap<>());

    for (final AsyncEvent event : events) {
      try {
        queueSemaphore.acquire();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      final var future = CompletableFuture.runAsync(
          new AsyncEventTask<>(
              event,
              taskContext,
              asyncProcessorContext,
              queueSemaphore
          ),
          executor
      );
      final var inFlightEvent = new InFlightEvent(future);
      inFlightForTask.put(event, inFlightEvent);

      future
          .whenComplete((r, t) -> {
            if (t == null) {
              inFlightForTask.remove(event);
              finalizingQueue.addFinalizableEvent(event);
            }
          })
          .exceptionally(processingException -> {
            inFlightEvent.setError(processingException);
            event.transitionToFailed(processingException);
            finalizingQueue.addFailedEvent(event, processingException);
            return null;
          })
          .exceptionally(processingException -> {
            // do this in separate stage to ensure we always catch a fatal exception, even
            // if we somehow hit another exception in the previous exception handling stage
            fatalExceptions.computeIfAbsent(inFlightKey, k -> processingException);
            return null;
          });
    }
  }

  public void shutdown() {
    // todo: make me more orderly, but you get the basic idea
    executor.shutdownNow();
  }

  static class InFlightEvent {
    private final CompletableFuture<Void> future;
    private Throwable error = null;

    private InFlightEvent(final CompletableFuture<Void> future) {
      this.future = future;
    }

    CompletableFuture<Void> future() {
      return future;
    }

    public synchronized Optional<Throwable> error() {
      return Optional.ofNullable(error);
    }

    public synchronized void setError(final Throwable error) {
      this.error = error;
    }
  }

  private static class AsyncEventTask<KOut, VOut> implements Runnable {

    private final AsyncEvent event;
    private final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext;
    private final AsyncUserProcessorContext<KOut, VOut> wrappingContext;
    private final Semaphore queueSemaphore;

    private AsyncEventTask(
        final AsyncEvent event,
        final ProcessingContext taskContext,
        final AsyncUserProcessorContext<KOut, VOut> userContext,
        final Semaphore queueSemaphore
    ) {
      this.event = event;
      this.wrappingContext = userContext;
      this.asyncThreadContext = new AsyncThreadProcessorContext<>(
          taskContext,
          event
      );
      this.queueSemaphore = queueSemaphore;
    }

    @Override
    public void run() {
      queueSemaphore.release();
      wrappingContext.setDelegateForAsyncThread(asyncThreadContext);
      event.transitionToProcessing();
      event.inputRecordProcessor().run();
      event.transitionToToFinalize();
    }
  }

  // TODO: think of a better name for this class
  private static class InFlightWorkKey {
    private final String processorName;
    private final int partition;

    private InFlightWorkKey(final String processorName, final int partition) {
      this.processorName = processorName;
      this.partition = partition;
    }

    private static InFlightWorkKey of(final String processorName, final int partition) {
      return new InFlightWorkKey(processorName, partition);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final InFlightWorkKey that = (InFlightWorkKey) o;
      return Objects.equals(processorName, that.processorName)
          && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(processorName, partition);
    }
  }
}
