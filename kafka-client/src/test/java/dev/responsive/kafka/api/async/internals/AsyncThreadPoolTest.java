package dev.responsive.kafka.api.async.internals;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.google.common.base.Throwables;
import dev.responsive.kafka.api.async.internals.contexts.AsyncUserProcessorContext;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.metrics.AsyncProcessorMetricsRecorder;
import dev.responsive.kafka.api.async.internals.queues.FinalizingQueue;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncThreadPoolTest {
  private static final String POOL_NAME = "pool";
  private static final int POOL_SIZE = 2;
  private static final int POOL_EVENT_LIMIT = 4;

  @Mock
  private AsyncUserProcessorContext<String, String> userContext;
  @Mock
  private ProcessingContext originalContext;
  @Mock
  private ProcessorRecordContext recordContext;
  private final FinalizingQueue finalizingQueue0 = new FinalizingQueue("fq", 0);
  private final FinalizingQueue finalizingQueue1 = new FinalizingQueue("fq", 1);

  private AsyncThreadPool pool;

  @BeforeEach
  public void setup() {
    pool = new AsyncThreadPool(
        POOL_NAME,
        POOL_SIZE,
        POOL_EVENT_LIMIT,
        mock(ResponsiveMetrics.class)
    );
    pool.maybeInitThreadPoolMetrics();
  }

  @AfterEach
  public void teardown() {
    pool.shutdown();
  }

  @Test
  public void shouldFinalizeEventWhenFinished()
      throws InterruptedException, TimeoutException, ExecutionException {
    // given:
    final var task = new TestTask();
    final var event = newEvent(task, 0);
    task.waitLatch.countDown();

    // when:
    schedule("processor", 0, finalizingQueue0, event);

    // then:
    final Map<AsyncEvent, AsyncThreadPool.InFlightEvent> inFlight
        = pool.getInFlight("processor", 0);
    final var inFlightEvent = inFlight.get(event);
    if (inFlightEvent != null) {
      final var future = inFlightEvent.future();
      future.get(10, TimeUnit.SECONDS);
    }
    assertThat(finalizingQueue0.waitForNextFinalizableEvent(1, TimeUnit.MINUTES), is(event));
  }

  @Test
  public void shouldSetFatalExceptionWhenUnexpectedExceptionThrown() throws InterruptedException {
    // given:
    final var exception = new RuntimeException("oops");
    doThrow(exception).when(userContext).setDelegateForAsyncThread(any());
    final var task1 = new TestTask();
    final var event = newEvent(task1, 0);

    // when:
    schedule("processor", 0, finalizingQueue0, event);

    // then:
    final Instant start = Instant.now();
    Optional<Throwable> caught;
    do {
      caught = pool.checkUncaughtExceptions("processor", 0);
      if (caught.isPresent()) {
        break;
      }
      Thread.sleep(100);
    } while (Duration.between(start, Instant.now()).compareTo(Duration.ofMinutes(1)) < 0);
    assertThat(caught.isPresent(), is(true));
    assertThat(caught.get(), instanceOf(FatalAsyncException.class));
    assertThat(Throwables.getRootCause(caught.get()), is(exception));
  }

  @Test
  public void shouldCancelInFlightWhenTaskClosed() {
    // given:
    final var task1 = new TestTask();
    final var event1 = newEvent(task1, 0);
    final var task2 = new TestTask();
    final var event2 = newEvent(task2, 0);
    final var task3 = new TestTask();
    final var event3 = newEvent(task3, 0);
    schedule("processor", 0, finalizingQueue0, event1, event2, event3);
    final var taskOtherProcessor = new TestTask();
    final var eventOtherProcessor = newEvent(taskOtherProcessor, 0);
    schedule("other", 0, finalizingQueue0, eventOtherProcessor);
    final var taskOtherPartition = new TestTask();
    final var eventOtherPartition = newEvent(taskOtherPartition, 1);
    schedule("processor", 1, finalizingQueue1, eventOtherPartition);
    final var processor0InFlight = Map.copyOf(pool.getInFlight("processor", 0));
    final var processor1InFlight = Map.copyOf(pool.getInFlight("processor", 1));
    final var other0InFlight = Map.copyOf(pool.getInFlight("other", 0));

    // when:
    pool.removeProcessor("processor", 0);

    // then:
    for (final var t : List.of(task1, task2, task3, taskOtherPartition, taskOtherProcessor)) {
      t.waitLatch.countDown();
    }
    assertThat(processor0InFlight.get(event3).future().isCancelled(), is(true));
    assertThat(processor1InFlight.get(eventOtherPartition).future().isCancelled(), is(false));
    assertThat(other0InFlight.get(eventOtherProcessor).future().isCancelled(), is(false));
  }

  @Test
  public void shouldNotRaiseFatalExceptionOnCancellation() {
    // given:
    final var task1 = new TestTask();
    final var event1 = newEvent(task1, 0);
    schedule("processor", 0, finalizingQueue0, event1);

    // when:
    pool.removeProcessor("processor", 0);

    // then:
    for (final var t : List.of(task1)) {
      t.waitLatch.countDown();
    }
    assertThat(
        pool.checkUncaughtExceptions("processor", 0),
        is(Optional.empty())
    );
  }

  private static final class TestTask implements Runnable {
    private final CountDownLatch waitLatch = new CountDownLatch(1);

    @Override
    public void run() {
      try {
        waitLatch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void schedule(
      final String processor,
      final int partition,
      final FinalizingQueue finalizingQueue,
      final AsyncEvent... events
  ) {
    pool.scheduleForProcessing(
        processor,
        new TaskId(0, partition),
        Arrays.asList(events),
        finalizingQueue,
        originalContext,
        userContext,
        mock(AsyncProcessorMetricsRecorder.class)
    );
  }

  private AsyncEvent newEvent(final TestTask task, final int partition) {
    final var event = new AsyncEvent(
        "event",
        new Record<>("k", "v", 0L),
        "async-processor",
        new TaskId(0, partition),
        recordContext,
        0L,
        0L,
        task,
        List.of()
    );
    event.transitionToToProcess();
    return event;
  }
}
