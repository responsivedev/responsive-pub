package dev.responsive.kafka.api.async.internals;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.metrics.AsyncProcessorMetricsRecorder;
import dev.responsive.kafka.api.async.internals.metrics.AsyncThreadPoolMetricsRecorder;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

public class AsyncThreadPool {
  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final Logger log;

  private final Supplier<AsyncThreadPoolMetricsRecorder> metricsRecorderSupplier;
  private AsyncThreadPoolMetricsRecorder metricsRecorder;
  private final ThreadPoolExecutor executor;
  private final Map<AsyncProcessorId, ConcurrentMap<AsyncEvent, InFlightEvent>> inFlight
      = new HashMap<>();
  private final Semaphore queueSemaphore;
  private final BlockingQueue<Runnable> processingQueue;

  // TODO: we don't really need to track this by processor/partition, technically an
  //  error anywhere is fatal for the StreamThread and all processors + all further
  //  processing should shut down ASAP to minimize ALOS overcounting
  private final Map<AsyncProcessorId, FatalAsyncException> fatalExceptions =
      new ConcurrentHashMap<>();

  private final AtomicInteger threadNameIndex = new AtomicInteger(0);

  public AsyncThreadPool(
      final String streamThreadName,
      final int threadPoolSize,
      final int maxQueuedEvents,
      final ResponsiveMetrics responsiveMetrics
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
    this.metricsRecorderSupplier = () -> new AsyncThreadPoolMetricsRecorder(
        responsiveMetrics,
        streamThreadName,
        processingQueue::size
    );
  }

  public boolean isEmpty(final String processorName, final TaskId taskId) {
    final var forTask = inFlight.get(AsyncProcessorId.of(processorName, taskId));
    if (forTask == null) {
      log.debug("No in-flight map found for {}[{}]", processorName, taskId);
      return true;
    }
    if (log.isTraceEnabled()) {
      log.trace("Found in-flight map for {}[{}]: {}",
          processorName,
          taskId,
          forTask.keySet().stream().map(AsyncEvent::toString).collect(Collectors.joining(", "))
      );
    }
    return forTask.isEmpty();
  }

  public void removeProcessor(final AsyncProcessorId asyncId) {
    log.debug("Removing {}[{}] from async thread pool", asyncId.processorName, asyncId.taskId);
    final Map<AsyncEvent, InFlightEvent> inFlightForTask = inFlight.remove(asyncId);


    if (inFlightForTask != null) {
      log.info("Cancelling {} pending records for {}[{}]",
               inFlightForTask.size(), asyncId.processorName, asyncId.taskId);
      inFlightForTask.values().forEach(f -> f.future().cancel(true));
    }
  }

  /**
   * Returns uncaught exceptions from processing async events. Such errors are always fatal
   *
   * @return any uncaught exceptions encountered during processing of input records,
   *         or {@link Optional#empty()} if there are none
   */
  public Optional<Throwable> checkUncaughtExceptions(
      final String processorName,
      final TaskId taskId
  ) {
    return Optional.ofNullable(fatalExceptions.get(AsyncProcessorId.of(processorName, taskId)));

  }

  @VisibleForTesting
  Map<AsyncEvent, InFlightEvent> getInFlight(final String processorName, final TaskId taskId) {
    return inFlight.get(AsyncProcessorId.of(processorName, taskId));
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
      final TaskId taskId,
      final List<AsyncEvent> events,
      final FinalizingQueue finalizingQueue,
      final ProcessingContext taskContext,
      final AsyncUserProcessorContext<KOut, VOut> asyncProcessorContext,
      final AsyncProcessorMetricsRecorder processorMetricsRecorder
  ) {
    if (metricsRecorder == null) {
      log.error("must call maybeInitThreadPoolMetrics before using pool");
      throw new IllegalStateException("must call maybeInitThreadPoolMetrics before using pool");
    }

    final var asyncProcessorId = AsyncProcessorId.of(processorName, taskId);
    final var inFlightForTask
        = inFlight.computeIfAbsent(asyncProcessorId, k -> new ConcurrentHashMap<>());

    for (final AsyncEvent event : events) {
      try {
        queueSemaphore.acquire();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
      final CompletableFuture<StreamsException> future = CompletableFuture.supplyAsync(
          new AsyncEventTask<>(
              event,
              taskContext,
              asyncProcessorContext,
              queueSemaphore,
              processorMetricsRecorder
          ),
          executor
      );
      final var inFlightEvent = new InFlightEvent(future);
      inFlightForTask.put(event, inFlightEvent);

      future
          .whenComplete((r, t) -> {
            inFlightForTask.remove(event);
          })
          .whenComplete((processingException, fatalException) -> {

            if (fatalException == null) {
              if (processingException == null) {
                finalizingQueue.addFinalizableEvent(event);
              } else {
                event.transitionToFailed(processingException);
                finalizingQueue.addFailedEvent(event, processingException);
              }
            }

            // Once we've successfully placed a failed event in the finalizing queue
            // then there's nothing more to handle by the async thread pool since the
            // StreamThread will process the exception from here
          })
          .exceptionally(fatalException -> {
            // do this alone & in separate stage to ensure we always catch a fatal exception, even
            // if we somehow hit another exception while handling an exception in a previous stage
            if (fatalException instanceof CompletionException
                && fatalException.getCause() instanceof CancellationException) {
              // when the task is cancelled (e.g. by removeProcessor) this method is called
              // with a CompletionException caused by a CancellationException. This is not
              // a failure, so don't store it in fatalExceptions
              throw (CompletionException) fatalException;
            }
            fatalExceptions.computeIfAbsent(
                asyncProcessorId,
                k -> new FatalAsyncException("Uncaught exception while handling", fatalException));
            if (fatalException instanceof RuntimeException) {
              throw (RuntimeException) fatalException;
            }
            throw new RuntimeException(fatalException);
          });
    }
  }

  /**
   * This is a complete hack to work around the fact that we cannot create the
   * metrics recorder from the constructor of this class. This is because the recorder
   * needs to know all the tags to register a metric. One of the tags is the client id
   * however this is only computed in the KafkaStreams constructor, so ResponsiveKafkaStreams
   * can only set it after the KafkaStreams constructor has returned. However thread pools
   * are created from the KafkaStreams constructor, when it initializes StreamThread instances.
   * Fixing this is a bit involved. For now, just initialize the recorder on the first call
   * to scheduleForProcessing.
   */
  public void maybeInitThreadPoolMetrics() {
    if (metricsRecorder == null) {
      metricsRecorder = metricsRecorderSupplier.get();
    }
  }

  public void shutdown() {
    if (metricsRecorder != null) {
      metricsRecorder.close();
    }
    executor.shutdownNow();
  }

  static class InFlightEvent {
    private final CompletableFuture<StreamsException> future;

    private InFlightEvent(final CompletableFuture<StreamsException> future) {
      this.future = future;
    }

    CompletableFuture<StreamsException> future() {
      return future;
    }
  }

  private static class AsyncEventTask<KOut, VOut> implements Supplier<StreamsException> {

    private final AsyncEvent event;
    private final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext;
    private final AsyncUserProcessorContext<KOut, VOut> wrappingContext;
    private final Semaphore queueSemaphore;
    private final AsyncProcessorMetricsRecorder metricsRecorder;

    private AsyncEventTask(
        final AsyncEvent event,
        final ProcessingContext taskContext,
        final AsyncUserProcessorContext<KOut, VOut> userContext,
        final Semaphore queueSemaphore,
        final AsyncProcessorMetricsRecorder metricsRecorder
    ) {
      this.event = event;
      this.wrappingContext = userContext;
      this.asyncThreadContext = new AsyncThreadProcessorContext<>(
          taskContext,
          event
      );
      this.queueSemaphore = queueSemaphore;
      this.metricsRecorder = metricsRecorder;
    }

    @Override
    public StreamsException get() {
      final long start = System.nanoTime();
      queueSemaphore.release();
      wrappingContext.setDelegateForAsyncThread(asyncThreadContext);
      event.transitionToProcessing();

      try {
        event.inputRecordProcessor().run();
      } catch (final RuntimeException e) {
        return new StreamsException("Exception caught during async processing", e, event.taskId());
      }

      event.transitionToToFinalize();
      metricsRecorder.recordEventProcess(System.nanoTime() - start);
      return null;
    }
  }
}
