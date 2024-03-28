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

package dev.responsive.kafka.api.async.internals.queues;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * A non-blocking queue for async events waiting to be passed from the StreamThread to
 * the async thread pool and scheduled for execution. This queue is not thread safe and
 * should be owned and exclusively accessed by the StreamThread. Events that are
 * processable -- that is, not blocked on previously scheduled events with
 * the same key that have not yet been fully processed -- will be polled from
 * this queue and then "scheduled" by passing them on to the {@link ProcessingQueue}
 * <p>
 * Threading notes:
 * -Should only be accessed from the StreamThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class SchedulingQueue<KIn> {

  private final Map<KIn, KeyStatus> blockedEvents = new HashMap<>();
  private final Queue<AsyncEvent> processableEvents = new LinkedList<>();

  /**
   * Mark the given key as unblocked and free up the next record with
   * the same key that's waiting to be scheduled.
   * Called upon the finalization of an async event with the given input key
   */
  public void unblockKey(final KIn key) {
    final KeyStatus keyStatus = getOrCreateKeyStatus(key);
    if (!keyStatus.isBlocked()) {
      throw new IllegalStateException("Attempted to unblock a key but it was not blocked");
    }

    final AsyncEvent nextProcessableEvent = keyStatus.nextEvent();
    if (nextProcessableEvent != null) {
      // If there are blocked events waiting, promote one but don't unblock
      processableEvents.offer(nextProcessableEvent);
    } else {
      blockedEvents.remove(key);
    }
  }

  /**
   * @return whether there are any remaining records in the queue which are currently
   *         ready for processing
   */
  public boolean hasProcessableRecord() {
    return !processableEvents.isEmpty();
  }

  /**
   * Get the next longest-waiting event that satisfies the constraint for processing, namely
   * that all previous records with the same {@link KIn key type} have been completed
   *
   * @return the next available event that is ready for processing
   *         or {@code null} if there are no processable records
   */
  public AsyncEvent poll() {
    return processableEvents.poll();
  }

  /**
   * Add a new input record to the queue. Records will be processing in modified FIFO
   * order; essentially picking up the next oldest record that is ready to be processed,
   * in other words, excluding those that are awaiting previous same-key records to complete.
   */
  public void offer(
      final AsyncEvent event
  ) {
    final KeyStatus keyStatus = getOrCreateKeyStatus(event.inputKey());
    if (keyStatus.isBlocked()) {
      keyStatus.addBlockedEvent(event);
    } else {
      keyStatus.block();
      processableEvents.offer(event);
    }
  }

  private KeyStatus getOrCreateKeyStatus(final KIn key) {
    return blockedEvents.computeIfAbsent(key, k -> new KeyStatus());
  }

  /**
   * Tracks the blocked events waiting to be scheduled and the current status
   * of this key, ie whether there is an in-flight event of the same key that
   * is currently blocking other events from being scheduled.
   * <p>
   * A KeyStatus, and all events with that input key, are considered blocked
   * if there is an async event currently in-flight with this key. An
   * event is "in-flight" from the moment it leaves the blockedEvents queue
   * until the moment it is finalized and marked done. An event that is in
   * the processableEvents queue but has not yet been pulled from the
   * SchedulingQueue and passed on to the AsyncThreadPool is still considered
   * to be "in-flight", and should block any other events with that key from
   * being added to the processableEvents queue.
   */
  private static class KeyStatus {
    private final Queue<AsyncEvent> blockedEvents = new LinkedList<>();
    private boolean hasInFlightEvent = false;

    private boolean isBlocked() {
      return hasInFlightEvent;
    }

    private void block() {
      hasInFlightEvent = true;
    }

    private AsyncEvent nextEvent() {
      return blockedEvents.poll();
    }

    private void addBlockedEvent(final AsyncEvent event) {
      if (!isBlocked()) {
        throw new IllegalStateException("Attempted to add event to blocked queue, but "
                                            + "this key is not currently blocked");
      }
      blockedEvents.add(event);
    }
  }
}
