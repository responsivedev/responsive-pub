package dev.responsive.utils;

import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public final class IntegrationTestUtils {
  private IntegrationTestUtils() {
  }

  public static void startAppAndAwaitRunning(
      final Duration timeout,
      final ResponsiveKafkaStreams... streams
  ) throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    final Condition onRunning = lock.newCondition();
    final AtomicBoolean[] running = new AtomicBoolean[streams.length];

    for (int i = 0; i < streams.length; i++) {
      running[i] = new AtomicBoolean(false);
      final int idx = i;
      final StateListener oldListener = streams[i].stateListener();
      final StateListener listener = (newState, oldState) -> {
        if (oldListener != null) {
          oldListener.onChange(newState, oldState);
        }

        lock.lock();
        try {
          running[idx].set(newState == State.RUNNING);
          onRunning.signalAll();
        } finally {
          lock.unlock();
        }
      };
      streams[i].setStateListener(listener);
    }

    for (final KafkaStreams stream : streams) {
      stream.start();
    }

    final long end = System.nanoTime() + timeout.toNanos();
    lock.lock();
    try {
      while (Arrays.stream(running).anyMatch(b -> !b.get())) {
        if (System.nanoTime() > end
            || !onRunning.await(end - System.nanoTime(), TimeUnit.NANOSECONDS)) {
          throw new TimeoutException("Not all streams were running after " + timeout);
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
