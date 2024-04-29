package dev.responsive.kafka.api.async.internals;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

public class AsyncThreadPool {
  private final ExecutorService executorService;
  private final ConcurrentMap<InFlightWorkKey, Map<AsyncEvent, Future<?>>> inFlight
      = new ConcurrentHashMap<>();

  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final Logger log;

  private final BlockingQueue<Runnable> processingQueue;

  private final AtomicInteger threadNameIndex = new AtomicInteger(0);

  public AsyncThreadPool(
      final String streamThreadName,
      final int threadPoolSize,
      final int maxQueuedEvents
  ) {
    final LogContext logPrefix
        = new LogContext(String.format("stream-thread [%s] ", streamThreadName));
    this.log = logPrefix.logger(AsyncThreadPool.class);
    this.processingQueue = new LinkedBlockingQueue<>(maxQueuedEvents);

    executorService = new ThreadPoolExecutor(
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

  boolean isEmpty(final String processorName, final int partition) {
    final var forTask = inFlight.get(InFlightWorkKey.of(processorName, partition));
    if (forTask == null) {
      log.debug("no in-flight map found for {}:{}", processorName, partition);
      return true;
    }
    if (log.isTraceEnabled()) {
      log.trace("found in-flight map for {}:{}: {}",
          processorName,
          partition,
          forTask.keySet().stream().map(AsyncEvent::toString).collect(Collectors.joining(", "))
      );
    }
    return forTask.isEmpty();
  }

  @VisibleForTesting
  Map<AsyncEvent, Future<?>> getInFlight(final String processorName, final int partition) {
    return inFlight.get(InFlightWorkKey.of(processorName, partition));
  }

  public void removeProcessor(final String processorName, final int partition) {
    log.info("clean up records for {}:{}", processorName, partition);
    final var key = InFlightWorkKey.of(processorName, partition);
    final Map<AsyncEvent, Future<?>> inFlightForTask = inFlight.remove(key);
    if (inFlightForTask != null) {
      inFlightForTask.values().forEach(f -> f.cancel(true));
    }
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
   * This is a blocking call, as it will wait until the processing queue has
   * enough space to accept a new event.
   */
  public <KOut, VOut> void scheduleForProcessing(
      final String processorName,
      final int taskId,
      final List<AsyncEvent> events,
      final FinalizingQueue finalizingQueue,
      final ProcessingContext taskContext,
      final AsyncUserProcessorContext<KOut, VOut> asyncProcessorContext
  ) {
    // todo: can also check in-flight for failed tasks
    for (final var e : events) {
      final var inFlightKey = InFlightWorkKey.of(processorName, taskId);
      inFlight.computeIfAbsent(inFlightKey, k -> new ConcurrentHashMap<>());
      final var inFlightForTask = inFlight.get(inFlightKey);
      if (inFlightForTask.isEmpty()) {
        log.debug("added in-flight map for task {}", taskId);
      }
      // hold the lock for the in-flight records so that we ensure that the future is inserted
      // before it is removed by the task
      synchronized (inFlightForTask) {
        final Future<?> future = executorService.submit(new AsyncEventTask<>(
            e,
            taskContext,
            asyncProcessorContext,
            finalizingQueue,
            inFlightForTask,
            log));
        inFlightForTask.put(e, future);
      }
    }
  }

  public void shutdown() {
    // todo: make me more orderly, but you get the basic idea
    executorService.shutdownNow();
  }

  private static class AsyncEventTask<KOut, VOut> implements Runnable {
    private final AsyncEvent event;
    private final ProcessingContext originalContext;
    private final AsyncUserProcessorContext<KOut, VOut> wrappingContext;
    private final FinalizingQueue finalizingQueue;
    private final Map<AsyncEvent, Future<?>> inFlightForTask;
    private final Logger log;

    private AsyncEventTask(
        final AsyncEvent event,
        final ProcessingContext originalContext,
        final AsyncUserProcessorContext<KOut, VOut> userContext,
        final FinalizingQueue finalizingQueue,
        final Map<AsyncEvent, Future<?>> inFlightForTask,
        final Logger log
    ) {
      this.event = event;
      this.originalContext = originalContext;
      this.wrappingContext = userContext;
      this.finalizingQueue = finalizingQueue;
      this.inFlightForTask = inFlightForTask;
      this.log = log;
    }

    @Override
    public void run() {
      wrappingContext.setDelegateForAsyncThread(new AsyncThreadProcessorContext<>(
          originalContext,
          event
      ));
      event.transitionToProcessing();
      event.inputRecordProcessor().run();
      synchronized (inFlightForTask) {
        final var previous = inFlightForTask.remove(event);
        if (previous == null) {
          if (log.isTraceEnabled()) {
            log.trace("no in-flight for event {}, remaining events with this key {}",
                event,
                inFlightForTask.keySet()
                    .stream()
                    .map(AsyncEvent::toString)
                    .collect(Collectors.joining(",")));
          }
          log.error("no in-flight records found for event");
          throw new IllegalStateException("no in-flight for event");
        } else {
          log.trace("found in-flight for event {}, remaining {}", event, inFlightForTask.size());
        }
      }
      // make sure to schedule for finalization only after removing from in-flight map
      finalizingQueue.scheduleForFinalization(event);
    }
  }

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
