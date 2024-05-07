package dev.responsive.kafka.api.async.internals;

import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.utils.LogContext;
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
      final List<AsyncEvent> events,
      final PendingEvents pendingEvents,
      final FinalizingQueue finalizingQueue,
      final AsyncUserProcessorContext<KOut, VOut> userProcessorContext
  ) {
    for (final AsyncEvent event : events) {
      final Future<?> future = executor.submit(
          new AsyncEventTask<>(event, pendingEvents, finalizingQueue, userProcessorContext)
      );
      pendingEvents.schedulePendingEvent(event, future);
    }
  }

  public void shutdown() {
    // todo: make me more orderly, but you get the basic idea
    executor.shutdownNow();
  }

  private static class AsyncEventTask<KOut, VOut> implements Runnable {
    private final AsyncEvent event;
    private final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext;
    private final AsyncUserProcessorContext<KOut, VOut> wrappingContext;
    private final PendingEvents pendingEvents;
    private final FinalizingQueue finalizingQueue;

    private AsyncEventTask(
        final AsyncEvent event,
        final PendingEvents pendingEvents,
        final FinalizingQueue finalizingQueue,
        final AsyncUserProcessorContext<KOut, VOut> userContext
    ) {
      this.event = event;
      this.pendingEvents = pendingEvents;
      this.finalizingQueue = finalizingQueue;
      this.wrappingContext = userContext;
      this.asyncThreadContext = new AsyncThreadProcessorContext<>(
          userContext.taskContext(),
          event
      );
    }

    @Override
    public void run() {
      try {

        wrappingContext.setDelegateForAsyncThread(asyncThreadContext);

        event.transitionToProcessing();
        event.inputRecordProcessor().run();
        event.transitionToToFinalize();

      } catch (final RuntimeException e) {
        event.transitionToToFailed(e);
        pendingEvents.registerFailedEvent(event);
      }
      finalizingQueue.scheduleForFinalization(event);

    }
  }

}
