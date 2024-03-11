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

import dev.responsive.kafka.api.async.internals.AsyncProcessorRecordContext;
import dev.responsive.kafka.api.async.internals.AsyncThread;
import dev.responsive.kafka.api.async.internals.records.ProcessableRecord;

import java.util.function.Consumer;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;

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

  private final OffsetOrderQueue<KIn, VIn, ProcessableRecord<KIn, VIn>> processableRecords =
      OffsetOrderQueue.nonBlockingQueue(null);

  // Effectively final, just has to wait for the processor's #init
  private TaskId taskId;

  // Some metadata is initialized late (since we have to wait for #init to get it)
  public void initialize(final TaskId taskId) {
    this.taskId = taskId;
  }

  /**
   * Schedules a processable record to be executed asynchronously by a thread from the async pool
   * <p>
   * To be executed by the StreamThread only
   */
  public void scheduleForProcessing(
      final Record<KIn, VIn> record,
      final AsyncProcessorRecordContext recordContext,
      final Runnable process,
      final Runnable processListener
  ) {
    processableRecords.put(new ProcessableRecord<>(
        record, recordContext, process, processListener)
    );
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

}
