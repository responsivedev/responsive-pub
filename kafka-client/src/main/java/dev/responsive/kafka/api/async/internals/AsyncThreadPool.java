package dev.responsive.kafka.api.async.internals;

import dev.responsive.kafka.api.async.internals.contexts.AsyncProcessorContext;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncThreadPool {
  private final ExecutorService executorService;
  private final Map<Integer, Map<AsyncEvent, Future<?>>> inFlight = new HashMap<>();

  public AsyncThreadPool(final String name, final int size) {
    // todo: set the thread names
    executorService = Executors.newFixedThreadPool(size);
  }

  public void removeProcessor(final int partition) {
    final Map<AsyncEvent, Future<?>> inFlightForTask = inFlight.get(partition);
    inFlightForTask.values().forEach(f -> f.cancel(true));
  }

  public <KOut, VOut> void scheduleForProcessing(
      final int taskId,
      final List<AsyncEvent> events,
      final FinalizingQueue finalizingQueue,
      final AsyncThreadProcessorContext<KOut, VOut> asyncThreadProcessorContext,
      final AsyncProcessorContext<KOut, VOut> asyncProcessorContext
      ) {
    // todo: can also check in-flight for failed tasks
    for (final var e : events) {
      final Future<?> future = executorService.submit(new AsyncEventTask<>(
          e,
          asyncThreadProcessorContext,
          asyncProcessorContext,
          finalizingQueue));
      inFlight.putIfAbsent(taskId, new HashMap<>());
      inFlight.get(taskId).put(e, future);
    }
  }

  public void shutdown() {
    // todo: make me more orderly, but you get the basic idea
    executorService.shutdownNow();
  }

  private static class AsyncEventTask<KOut, VOut> implements Runnable {
    private final AsyncEvent event;
    private final AsyncThreadProcessorContext<KOut, VOut> context;
    private final AsyncProcessorContext<KOut, VOut> wrappingContext;
    private final FinalizingQueue finalizingQueue;

    private AsyncEventTask(
        final AsyncEvent event,
        final AsyncThreadProcessorContext<KOut, VOut> context,
        final AsyncProcessorContext<KOut, VOut> wrappingContext,
        final FinalizingQueue finalizingQueue
    ) {
      this.event = event;
      this.context = context;
      this.wrappingContext = wrappingContext;
      this.finalizingQueue = finalizingQueue;
    }

    @Override
    public void run() {
      context.prepareToProcessNewEvent(event);
      wrappingContext.setDelegateForCurrentThread(context);
      event.inputRecordProcessor().run();
      finalizingQueue.scheduleForFinalization(event);
    }
  }
}
