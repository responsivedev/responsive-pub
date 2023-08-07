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

    final var p0 = new TestWriter(latches, 0, 1, true, exec, tracker);
    final var p1 = new TestWriter(latches, 1, 2, true, exec, tracker);
    final var p2 = new TestWriter(latches, 2, 2, true, exec, tracker);

    // When:
    final RemoteWriteResult result = CommitBuffer.drain(
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
  public void shouldExecAllWritesEvenWhenEarlierWriteFails() throws InterruptedException {
    // Given:
    final var latches = Map.of(
        0, new CountDownLatch(0),
        1, new CountDownLatch(1),
        2, new CountDownLatch(1)
    );
    final ExecutorService exec = Executors.newFixedThreadPool(4);

    final ConcurrentLinkedDeque<Integer> tracker = new ConcurrentLinkedDeque<>();
    final var p0 = new TestWriter(latches, 0, 1, false, exec, tracker);
    final var p1 = new TestWriter(latches, 1, 2, true, exec, tracker);
    final var p2 = new TestWriter(latches, 2, 0, true, exec, tracker);

    // When:
    final RemoteWriteResult result = CommitBuffer.drain(
        // 0 fails, which should allow 1 to complete and 2 to run
        List.of(p1, p0, p2),
        TestWriter::get,
        TestWriter::partition,
        100,
        2
    );

    // Then:
    assertThat(result.wasApplied(), is(false));
    assertThat(result.getPartition(), is(0));
    assertThat(tracker, hasSize(3));
    assertThat(tracker, contains(0, 1, 2));

    exec.shutdown();
  }

  static class TestWriter implements Supplier<CompletionStage<RemoteWriteResult>> {

    private final Map<Integer, CountDownLatch> latches;
    private final int partition;
    private final int release;
    private final boolean success;
    private final Executor exec;
    private final ConcurrentLinkedDeque<Integer> orderTracker;

    public TestWriter(
        final Map<Integer, CountDownLatch> latches,
        final int partition,
        final int release,
        final boolean success,
        final Executor exec,
        final ConcurrentLinkedDeque<Integer> successTracker) {
      this.latches = latches;
      this.partition = partition;
      this.release = release;
      this.success = success;
      this.exec = exec;
      this.orderTracker = successTracker;
    }

    @Override
    public CompletionStage<RemoteWriteResult> get() {
      return supplyAsync(
          awrSupplier(latches, partition, release, success, orderTracker), exec
      );
    }

    public int partition() {
      return partition;
    }
  }

  private static Supplier<RemoteWriteResult> awrSupplier(
      final Map<Integer, CountDownLatch> latches,
      final int partition,
      final int release,
      final boolean success,
      final ConcurrentLinkedDeque<Integer> tracker
  ) {
    return () -> {
      try {
        LOG.info("Started execution of {}", partition);
        latches.get(partition).await();
        tracker.add(partition);
        latches.get(release).countDown();

        if (success) {
          LOG.info("Finished execution of {}.. Releasing {}", partition, release);
          return RemoteWriteResult.success(partition);
        } else {
          LOG.info("Failed execution of {}.. Releasing {}", partition, release);
          return RemoteWriteResult.failure(partition);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted partition {}", partition);
        throw new CompletionException(e);
      }
    };
  }


}
