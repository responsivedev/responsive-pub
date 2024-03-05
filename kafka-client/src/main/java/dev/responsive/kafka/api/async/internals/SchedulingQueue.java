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

package dev.responsive.kafka.api.async.internals;

import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.kafka.streams.processor.api.Record;

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
 */
@NotThreadSafe
public class SchedulingQueue<KIn, VIn> {

  private final RecordNode<KIn, VIn> head;
  private final RecordNode<KIn, VIn> tail;

  public SchedulingQueue() {
    this.head = new RecordNode<>(null, null, null, null);
    this.tail = new RecordNode<>(null, null, null, null);

    head.next = tail;
    tail.previous = head;
  }

  /**
   * Get the next oldest record that satisfies the constraint for processing, namely
   * that all previous records with the same {@link KIn key type} have been completed
   *
   * @return the next available record that is ready for processing
   *         or {@code null} if there are no processable records
   */
  public Record<KIn, VIn> poll() {
    final RecordNode<KIn, VIn> nextProcessableRecordNode = nextProcessableRecordNode();
    return nextProcessableRecordNode != null
        ? nextProcessableRecordNode.record
        : null;
  }

  /**
   * @return whether there are any remaining records in the queue which are currently
   *         ready for processing
   */
  public boolean hasNext() {
    return nextProcessableRecordNode() != null;
  }

  private RecordNode<KIn, VIn> nextProcessableRecordNode() {
    RecordNode<KIn, VIn> current = head.next;
    while (current != tail) {
      if (current.isProcessable()) {
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
   *
   * @param record a new record to schedule for processing
   */
  public void offer(final Record<KIn, VIn> record) {
    addToTail(record);
  }

  private void addToTail(final Record<KIn, VIn> record) {
    final RecordNode<KIn, VIn> node = new RecordNode<>(record, k -> {throw new RuntimeException("Need to implement Predicate!");}, tail, tail.previous);
    tail.previous.next = node;
    tail.previous = node;
  }

  private Record<KIn, VIn> remove(final RecordNode<KIn, VIn> node) {
    node.next.previous = node.previous;
    node.previous.next = node.next;
    return node.record;
  }

  private static class RecordNode<KIn, VIn> {
    private final Record<KIn, VIn> record;
    private final Predicate<KIn> isProcessable;

    private RecordNode<KIn, VIn> next;
    private RecordNode<KIn, VIn> previous;

    public RecordNode(
        final Record<KIn, VIn> record,
        final Predicate<KIn> isProcessable,
        final RecordNode<KIn, VIn> next,
        final RecordNode<KIn, VIn> previous
    ) {
      this.record = record;
      this.isProcessable = isProcessable;
      this.next = next;
      this.previous = previous;
    }

    public boolean isProcessable() {
      return isProcessable.test(record.key());
    }
  }
}
