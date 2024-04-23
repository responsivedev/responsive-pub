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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

public class AsyncThreadPool {
  private final ExecutorService executorService;
  private final Map<InFlightWorkKey, Map<AsyncEvent, Future<?>>> inFlight = new HashMap<>();

  public static final String ASYNC_THREAD_NAME = "AsyncThread";

  private final Logger log;
  private final AtomicInteger threadNameIndex = new AtomicInteger(0);

  public AsyncThreadPool(
      final String streamThreadName,
      final int size) {
    final LogContext logPrefix
        = new LogContext(String.format("stream-thread [%s] ", streamThreadName));
    this.log = logPrefix.logger(AsyncThreadPool.class);
    // todo: set the thread names
    executorService = Executors.newFixedThreadPool(size, r -> {
      final var t = new Thread(r);
      t.setDaemon(true);
      t.setName(
          streamThreadName + "-" + ASYNC_THREAD_NAME + "-" + threadNameIndex.getAndIncrement());
      return t;
    });
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
      final Future<?> future = executorService.submit(new AsyncEventTask<>(
          e,
          taskContext,
          asyncProcessorContext,
          finalizingQueue));
      final var inFlightKey = InFlightWorkKey.of(processorName, taskId);
      inFlight.putIfAbsent(inFlightKey, new HashMap<>());
      inFlight.get(inFlightKey).put(e, future);
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

    private AsyncEventTask(
        final AsyncEvent event,
        final ProcessingContext originalContext,
        final AsyncUserProcessorContext<KOut, VOut> userContext,
        final FinalizingQueue finalizingQueue
    ) {
      this.event = event;
      this.originalContext = originalContext;
      this.wrappingContext = userContext;
      this.finalizingQueue = finalizingQueue;
    }

    @Override
    public void run() {
      wrappingContext.setDelegateForAsyncThread(new AsyncThreadProcessorContext<>(
          originalContext,
          event
      ));
      event.transitionToProcessing();
      event.inputRecordProcessor().run();
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
