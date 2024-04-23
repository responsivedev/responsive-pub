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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import dev.responsive.kafka.api.async.internals.QueuedEvents;
import org.junit.Test;

public class QueuedEventsTest {

  private static final int MAX_EVENTS = 5;

  private final QueuedEvents queuedEvents = new QueuedEvents(MAX_EVENTS);

  @Test
  public void shouldBeFullAfterEnqueuingMaxEvents() {
    // Given:
    final int partition = 1;
    queuedEvents.registerPartition(partition);

    // When:
    for (int i = 0; i < MAX_EVENTS; ++i) {
      queuedEvents.enqueueEvent(partition);
    }

    // Then:
    assertThat(queuedEvents.canEnqueue(), equalTo(false));
  }

  @Test
  public void shouldNotBeFullAfterDequeuingAnEvent() {
    // Given:
    final int partition = 1;
    queuedEvents.registerPartition(partition);

    for (int i = 0; i < MAX_EVENTS; ++i) {
      queuedEvents.enqueueEvent(partition);
    }

    // When:
    queuedEvents.dequeueEvent(partition);

    // Then:
    assertThat(queuedEvents.canEnqueue(), equalTo(true));
  }

  @Test
  public void shouldDequeueAllEventsForUnregisteredPartition() {
    // Given:
    final int partition = 1;
    queuedEvents.registerPartition(partition);

    for (int i = 0; i < MAX_EVENTS; ++i) {
      queuedEvents.enqueueEvent(partition);
    }
    assertThat(queuedEvents.canEnqueue(), equalTo(false));

    // When:
    queuedEvents.unregisterPartition(partition);

    // Then:
    assertThat(queuedEvents.isEmpty(), equalTo(true));
    assertThat(queuedEvents.canEnqueue(), equalTo(true));
  }

  @Test
  public void shouldBeAbleToEnqueueMaxEventsForSamePartitionAfterUnregistering() {
    // Given:
    final int partition = 1;
    queuedEvents.registerPartition(partition);

    for (int i = 0; i < MAX_EVENTS; ++i) {
      queuedEvents.enqueueEvent(partition);
    }
    assertThat(queuedEvents.canEnqueue(), equalTo(false));

    // When:
    queuedEvents.unregisterPartition(partition);
    assertThat(queuedEvents.isEmpty(), equalTo(true));

    // Then:
    for (int i = 0; i < MAX_EVENTS; ++i) {
      queuedEvents.enqueueEvent(partition);
    }
    assertThat(queuedEvents.isEmpty(), equalTo(false));
    assertThat(queuedEvents.canEnqueue(), equalTo(false));
  }

  @Test
  public void shouldThrowWhenEnqueueEventForUnregisteredPartition() {
    assertThrows(IllegalStateException.class, () -> queuedEvents.enqueueEvent(1));
  }

  @Test
  public void shouldThrowWhenDequeueEventForUnregisteredPartition() {
    assertThrows(IllegalStateException.class, () -> queuedEvents.dequeueEvent(1));
  }

  @Test
  public void shouldThrowWhenRegisterPartitionThatExists() {
    // Given:
    queuedEvents.registerPartition(1);

    // Then:
    assertThrows(IllegalStateException.class, () -> queuedEvents.registerPartition(2));
  }

  @Test
  public void shouldThrowWhenUnregisterPartitionThatDoesNotExist() {
    assertThrows(IllegalStateException.class, () -> queuedEvents.unregisterPartition(1));
  }

}
