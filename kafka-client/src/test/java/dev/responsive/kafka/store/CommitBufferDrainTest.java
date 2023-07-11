/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.store;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitBufferDrainTest {

  private static final Logger LOG = LoggerFactory.getLogger(CommitBufferDrainTest.class);

  @Test
  public void shouldDrainWithMaxConcurrencyWhenAllSucceed() {
    // Given:
    final var latches = Map.of(
        0, new CountDownLatch(0),
        1, new CountDownLatch(1),
        2, new CountDownLatch(1)
    );
    final ExecutorService exec = Executors.newFixedThreadPool(4);

    final ConcurrentLinkedDeque<Integer> tracker = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<Integer> interrupted = new ConcurrentLinkedDeque<>();

    final var p0 = new TestWriter(latches, 0, 1, true, exec, tracker, interrupted);
    final var p1 = new TestWriter(latches, 1, 2, true, exec, tracker, interrupted);
    final var p2 = new TestWriter(latches, 2, 2, true, exec, tracker, interrupted);

    // When:
    final AtomicWriteResult result = CommitBuffer.drain(
        // p2 blocks until p1 completes, this tests to ensure that
        // we run p1 when p0 completes instead of when the entire
        // mini-batch of [p0, p2] completes
        List.of(p0, p2, p1),
        TestWriter::get,
        TestWriter::partition,
        100,
        2
    );

    // Then:
    assertThat(result.wasApplied(), is(true));
    assertThat(result.getPartition(), is(100));
    assertThat(tracker.toArray(Integer[]::new), is(new Integer[]{0, 1, 2}));
    assertThat(interrupted, hasSize(0));

    exec.shutdown();
  }

  @Test
  public void shouldCancelOngoingAndNotContinueDrainOnFailure() throws InterruptedException {
    // Given:
    final var latches = Map.of(
        0, new CountDownLatch(0),
        1, new CountDownLatch(1),
        2, new CountDownLatch(1)
    );
    final ExecutorService exec = Executors.newFixedThreadPool(4);

    final ConcurrentLinkedDeque<Integer> tracker = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<Integer> interrupted = new ConcurrentLinkedDeque<>();
    final var p0 = new TestWriter(latches, 0, 1, false, exec, tracker, interrupted);
    final var p1 = new TestWriter(latches, 1, 2, true, exec, tracker, interrupted);
    final var p2 = new TestWriter(latches, 2, 0, true, exec, tracker, interrupted);

    // When:
    final AtomicWriteResult result = CommitBuffer.drain(
        // 0 fails, which should cancel 1 and 2 should never have run
        List.of(p1, p0, p2),
        TestWriter::get,
        TestWriter::partition,
        100,
        2
    );

    // Then:
    assertThat(result.wasApplied(), is(false));
    assertThat(result.getPartition(), is(0));
    assertThat(tracker, hasSize(1));
    assertThat(tracker, contains(0));

    exec.shutdownNow();

    // we should count down 1 when it is interrupted
    latches.get(1).await();

    // 2 should never have run, so it isn't interrupted
    assertThat(interrupted.size(), is(1));
    assertThat(interrupted, contains(1));
  }

  static class TestWriter implements Supplier<CompletionStage<AtomicWriteResult>> {

    private final Map<Integer, CountDownLatch> latches;
    private final int partition;
    private final int release;
    private final boolean success;
    private final Executor exec;
    private final ConcurrentLinkedDeque<Integer> orderTracker;
    private final ConcurrentLinkedDeque<Integer> interrupted;

    public TestWriter(
        final Map<Integer, CountDownLatch> latches,
        final int partition,
        final int release,
        final boolean success,
        final Executor exec,
        final ConcurrentLinkedDeque<Integer> successTracker,
        final ConcurrentLinkedDeque<Integer> interrupted) {
      this.latches = latches;
      this.partition = partition;
      this.release = release;
      this.success = success;
      this.exec = exec;
      this.orderTracker = successTracker;
      this.interrupted = interrupted;
    }

    @Override
    public CompletionStage<AtomicWriteResult> get() {
      return supplyAsync(
          awrSupplier(latches, partition, release, success, orderTracker, interrupted), exec
      );
    }

    public int partition() {
      return partition;
    }
  }

  private static Supplier<AtomicWriteResult> awrSupplier(
      final Map<Integer, CountDownLatch> latches,
      final int partition,
      final int release,
      final boolean success,
      final ConcurrentLinkedDeque<Integer> tracker,
      final ConcurrentLinkedDeque<Integer> interrupted
  ) {
    return () -> {
      try {
        LOG.info("Started execution of {}", partition);
        latches.get(partition).await();
        tracker.add(partition);

        if (success) {
          LOG.info("Finished execution of {}.. Releasing {}", partition, release);
          latches.get(release).countDown();
          return AtomicWriteResult.success(partition);
        } else {
          LOG.info("Failed execution of {}.. not releasing {}", partition, release);
          return AtomicWriteResult.failure(partition);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted partition {}", partition);
        interrupted.add(partition);
        latches.get(partition).countDown(); // count yourself down
        throw new CompletionException(e);
      }
    };
  }


}
