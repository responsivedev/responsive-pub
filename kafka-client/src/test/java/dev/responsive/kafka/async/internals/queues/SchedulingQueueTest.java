/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.async.internals.queues;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import dev.responsive.kafka.api.async.internals.queues.SchedulingQueue;
import dev.responsive.kafka.testutils.AsyncTestEvent;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

public class SchedulingQueueTest {

  private static final String LOG_PREFIX = "test";
  private static final long DEFAULT_QUEUE_SIZE = Long.MAX_VALUE;

  private final SchedulingQueue<String> queue = new SchedulingQueue<>(
      LOG_PREFIX, DEFAULT_QUEUE_SIZE
  );

  @Test
  public void shouldReturnNullWhenSameKeyEventIsInFlight() {
    // Given:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("A", "a2"));

    // When:
    queue.poll(); // in-flight record

    // Then:
    assertNull(queue.poll());
  }

  @Test
  public void shouldReturnBlockedEventAfterKeyIsUnblocked() {
    // Given:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("A", "a2"));

    queue.poll(); // remove 1st event but don't unblock it yet
    assertNull(queue.poll()); // only queued event is still blocked

    // When:
    queue.unblockKey("A");

    // Then:
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("A", "a2"))); // only queued event is still blocked
  }

  @Test
  public void shouldReturnOnlyUnblockedEventsInFIFOOrder() {
    // Given:
    queue.offer(new AsyncTestEvent("foo", "f1"));
    queue.offer(new AsyncTestEvent("foo", "f2"));
    queue.offer(new AsyncTestEvent("bar", "b1"));
    queue.offer(new AsyncTestEvent("bar", "b2"));
    queue.offer(new AsyncTestEvent("cat", "c1"));
    queue.offer(new AsyncTestEvent("cat", "c2"));

    // Then:
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("foo", "f1")));
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("bar", "b1")));
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("cat", "c1")));
    assertNull(queue.poll());
  }

  @Test
  public void shouldReturnEventsInUnblockingOrder() {
    // Given:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("B", "b1"));
    queue.offer(new AsyncTestEvent("C", "c1"));

    queue.poll();
    queue.poll();
    queue.poll();
    assertThat(queue.isEmpty(), is(true));

    // When:
    queue.offer(new AsyncTestEvent("A", "a2"));
    queue.offer(new AsyncTestEvent("B", "b2"));
    queue.offer(new AsyncTestEvent("C", "c2"));

    queue.unblockKey("C");
    queue.unblockKey("B");
    queue.unblockKey("A");

    // Then:
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("C", "c2")));
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("B", "b2")));
    assertThat(queue.poll().inputRecord(), is(new KeyValue<>("A", "a2")));
    assertNull(queue.poll());
  }

  @Test
  public void shouldReturnNullWhenNothingAdded() {
    // Given:

    // Then:
    assertNull(queue.poll());
  }

  @Test
  public void shouldReturnNullAfterAllEventsPolled() {
    // Given:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("A", "a2"));
    queue.offer(new AsyncTestEvent("B", "b1"));

    // When:
    queue.poll(); // polls a1
    queue.poll(); // polls b1
    queue.unblockKey("A");
    queue.poll(); // polls a2

    // Then:
    assertNull(queue.poll());
  }

  @Test
  public void shouldReturnTrueFromIsFullWhenAtMaxSizeWithAllSameKey() {
    // Given:
    final long maxEvents = 3;
    final SchedulingQueue<String> queue = new SchedulingQueue<>(LOG_PREFIX, maxEvents);

    // When:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("A", "a2"));
    queue.offer(new AsyncTestEvent("A", "a3"));

    // Then:
    assertThat(queue.isFull(), is(true));
  }

  @Test
  public void shouldReturnTrueFromIsFullWhenAtMaxSizeWithAllDifferentKeys() {
    // Given:
    final long maxEvents = 3;
    final SchedulingQueue<String> queue = new SchedulingQueue<>(LOG_PREFIX, maxEvents);

    // When:
    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("B", "b1"));
    queue.offer(new AsyncTestEvent("C", "c1"));

    // Then:
    assertThat(queue.isFull(), is(true));
  }

  @Test
  public void shouldReturnFalseFromIsFullWhenEventisPolled() {
    // Given:
    final long maxEvents = 3;
    final SchedulingQueue<String> queue = new SchedulingQueue<>(LOG_PREFIX, maxEvents);

    queue.offer(new AsyncTestEvent("A", "a1"));
    queue.offer(new AsyncTestEvent("A", "a2"));
    queue.offer(new AsyncTestEvent("B", "b1"));
    assertThat(queue.isFull(), is(true));

    // When:
    queue.poll();
    
    // Then:
    assertThat(queue.isFull(), is(false));
  }

  @Test
  public void shouldThrowWhenAddingToFullQueue() {
    // Given
    final long maxEvents = 1;
    final SchedulingQueue<String> queue = new SchedulingQueue<>(LOG_PREFIX, maxEvents);
    queue.offer(new AsyncTestEvent("A", "a1"));

    assertThrows(IllegalStateException.class, () -> queue.offer(new AsyncTestEvent("A", "a2")));
  }

  @Test
  public void shouldThrowWhenUnblockingKeyThatIsNotBlocked() {
    assertThrows(IllegalStateException.class, () -> queue.unblockKey("A"));
  }

}
