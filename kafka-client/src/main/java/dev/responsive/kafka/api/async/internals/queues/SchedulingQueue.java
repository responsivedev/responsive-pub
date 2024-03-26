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
import dev.responsive.kafka.api.async.internals.events.ScheduleableRecord;
import java.util.HashMap;
import java.util.Map;

// TODO:
//  1) implement predicate/isProcessable checking,
//  2) potentially save "cursor" in dll and have it automatically "reset" to the next oldest
//  node if/when they go from un-processable to processable via completion of pending records
//  3) make #poll blocking with Condition to notify when record becomes processable
/**
 * A non-blocking queue for input records waiting to be passed from the StreamThread to
 * the async thread pool and scheduled for execution. This queue is not thread safe and
 * should be owned and exclusively accessed by the StreamThread. Records that are
 * processable -- that is, not blocked on previous records with the same key that have
 * not yet been fully processed -- will be polled from this queue and then passed on to
 * the async thread pool by the StreamThread.
 * <p>
 * We implement a custom doubly-linked list to enable conditional queue-like semantics
 * with efficient arbitrary removal of the first element that meets the condition.
 * <p>
 * Threading notes:
 * -Should only be accessed from the StreamThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class SchedulingQueue<KIn, VIn> {

  /**
   * TODO: idea
   *
   * class EventKeyStatus {
   *   Queue<AsyncEvent> waitingEvents;
   *   boolean inFlightEvent;
   *
   *   set inFlight to true when an event of this key is polled from the PriorityQueue
   *
   *   when adding new events, check
   *   if (inFlightEvent ||   PriorityQueue.containsKEy(key)) --> add to waitingEvents queue
   *   else --> add to PriorityQueue
   *
   *   when an event is marked done, set inFlightEvent to false and move the first event in
   *   waitingEvents from the  key-specific Queue to the PriorityQueue
   * }
   *
   * use a PriorityQueue<AsyncEvent> that sorts according to offset (and secondarily topic -- but
   * how do we handle punctuator-created records??)
   * This PriorityQueue would only contain one event per key, and only events that are processable.
   * Every time an in-flight record is completed, we
   *
   * We also have a Map<Key -> Queue<AsyncEvent>> with ALL events awaiting scheduling, with each
   * key pointing to a queue/linked list with the events of the same key in offset order
   *
   * When an event is marked done by the StreamThread, it just passes it in here and we
   * will poll the next event from that key's queue and replace the
   */

  private final EventNode head;
  private final EventNode tail;
  private final Map<KIn, EventNode> inFlightNodesByKey = new HashMap<>();

  public SchedulingQueue() {
    this.head = new EventNode(null, null, null);
    this.tail = new EventNode(null, null, null);

    head.next = tail;
    tail.previous = head;
  }

  /**
   * @return whether there are any remaining records in the queue which are currently
   *         ready for processing
   *         Note: this differs from {@link #isEmpty()} in that it tests whether there
   *         are processable records, so it's possible for #isEmpty to return false
   *         while this also returns false
   */
  public boolean hasProcessableRecord() {
    return nextProcessableRecordNode() != null;
  }

  /**
   * @return true iff there are any pending records, whether or not they're processable
   *         If you want to know if there are any available records that are actually
   *         ready for processing, use {@link #hasProcessableRecord()} instead
   */
  public boolean isEmpty() {
    return head == tail;
  }

  /**
   * Get the next longest-waiting event that satisfies the constraint for processing, namely
   * that all previous records with the same {@link KIn key type} have been completed
   *
   * @return the next available event that is ready for processing
   *         or {@code null} if there are no processable records
   */
  public AsyncEvent<KIn, VIn> poll() {
    final EventNode nextProcessableRecordNode = nextProcessableRecordNode();
    return nextProcessableRecordNode != null
        ? nextProcessableRecordNode.asyncEvent
        : null;
  }

  private EventNode nextProcessableRecordNode() {
    EventNode current = head.next;
    while (current != tail) {

      if (canBeScheduled(current)) {
        remove(current);

        return current;
      } else {
        current = current.next;
      }
    }
    return null;
  }

  /**
   * Add a new input record to the queue. Records will be processing in modified FIFO
   * order; essentially picking up the next oldest record that is ready to be processed,
   * in other words, excluding those that are awaiting previous same-key records to complete.
   */
  public void put(
      final AsyncEvent<KIn, VIn> newAsyncEvent
  ) {
    addToTail(newAsyncEvent);
  }

  private void addToTail(final AsyncEvent<KIn, VIn> asyncEvent) {
    final EventNode node = new EventNode(
        asyncEvent,
        canBeScheduled(asyncEvent.inputKey()),
        tail,
        tail.previous
    );
    tail.previous.next = node;
    tail.previous = node;
  }

  private AsyncEvent remove(final EventNode node) {
    node.next.previous = node.previous;
    node.previous.next = node.next;
    return node.asyncEvent;
  }


  private boolean canBeScheduled(final KIn eventKey) {
    //TODO -- are there any other records scheduled ahead of this one OR a currently pending record?
    return !inFlightNodesByKey.containsKey(eventKey);
  }

  private class EventNode {
    private final AsyncEvent<KIn, VIn> asyncEvent;

    private EventNode next;
    private EventNode previous;

    public EventNode(
        final AsyncEvent<KIn, VIn> asyncEvent,
        final EventNode next,
        final EventNode previous
    ) {
      this.asyncEvent = asyncEvent;
      this.next = next;
      this.previous = previous;
    }

  }
}
