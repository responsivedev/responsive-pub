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

import dev.responsive.kafka.api.async.internals.AsyncThread;
import dev.responsive.kafka.api.async.internals.records.ProcessableRecord;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;

/**
 * A queue for processable records that have been scheduled and are waiting to be
 * picked up and executed by an {@link AsyncThread} from the pool. Essentially the
 * input queue to the async processing thread pool
 * <p>
 * Threading notes:
 * -Produces to queue --> StreamThread
 * -Consumes from queue --> AsyncThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class ProcessingQueue<KIn, VIn> {

  // Effectively final, just has to wait for the processor's #init
  private TaskId taskId;
  private final BlockingQueue<ProcessableRecord<KIn, VIn>> processableRecords;

  public ProcessingQueue() {
    this.processableRecords = new PriorityBlockingQueue<>(0, offsetOrderComparator());
  }

  // Some metadata is initialized late (since we have to wait for #init to get it)
  public void initialize(final TaskId taskId) {
    this.taskId = taskId;
  }

  /**
   * Schedules a processable record to be executed asynchronously by a thread from the async pool
   * <p>
   * To be executed by the StreamThread only
   */
  public void scheduleForProcessing(final ProcessableRecord<KIn, VIn> record) {
    boolean addedToQueue = processableRecords.offer(record);
    while (!addedToQueue) {
      // TODO:
      //  1. log a warning
      //  2. use a Condition to avoid busy waiting
      try {
        Thread.sleep(1_000L);
      } catch (final InterruptedException e) {
        throw new StreamsException("Interrupted while trying to schedule a record for processing",
                                   taskId);
      }
      addedToQueue = processableRecords.offer(record);
    }
  }

  /**
   * Returns the next processable record with the lowest offset, if available.
   * If there are no processable records in the queue, blocks until we get one.
   * <p>
   * To be executed by an AsyncThread only.
   */
  public ProcessableRecord<KIn, VIn> poll() {
    return processableRecords.poll();
  }

  public static <KIn, VIn> Comparator<ProcessableRecord<KIn, VIn>> offsetOrderComparator() {
    final Comparator<ProcessableRecord<KIn, VIn>> comparingOffsets =
        Comparator.comparingLong(r -> r.recordContext().offset());

    return comparingOffsets.thenComparing(r -> r.recordContext().topic());
  }
}
