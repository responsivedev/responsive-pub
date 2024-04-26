package dev.responsive.kafka.api.async.internals;

import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

public class AsyncThreadPool {
  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final Logger log;

  private final ThreadPoolExecutor executor;
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

    executor = new ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
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
  public <KOut, VOut> List<CompletableFuture<AsyncEvent>> scheduleForProcessing(
      final List<AsyncEvent> events,
      final ProcessingContext taskContext,
      final AsyncUserProcessorContext<KOut, VOut> asyncProcessorContext
  ) {
    final List<CompletableFuture<AsyncEvent>> futures = new ArrayList<>();
    for (final var e : events) {
      final var task = new AsyncEventTask<>(
          e,
          taskContext,
          asyncProcessorContext
      );
      final CompletableFuture<AsyncEvent> cf = CompletableFuture.supplyAsync(task::doRun, executor);
      futures.add(cf);
    }
    return futures;
  }

  public void shutdown() {
    // todo: make me more orderly, but you get the basic idea
    executor.shutdownNow();
  }

  private static class AsyncEventTask<KOut, VOut> implements Runnable {
    private final AsyncEvent event;
    private final ProcessingContext originalContext;
    private final AsyncUserProcessorContext<KOut, VOut> wrappingContext;

    private AsyncEventTask(
        final AsyncEvent event,
        final ProcessingContext originalContext,
        final AsyncUserProcessorContext<KOut, VOut> userContext
    ) {
      this.event = event;
      this.originalContext = originalContext;
      this.wrappingContext = userContext;
    }

    @Override
    public void run() {
      doRun();
    }

    public AsyncEvent doRun() {
      wrappingContext.setDelegateForAsyncThread(new AsyncThreadProcessorContext<>(
          originalContext,
          event
      ));
      event.transitionToProcessing();
      event.inputRecordProcessor().run();
      event.transitionToToFinalize();
      return event;
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
