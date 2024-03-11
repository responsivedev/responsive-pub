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
import dev.responsive.kafka.api.async.internals.records.ForwardableRecord;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A queue that holds on to records that are ready to be forwarded by the StreamThread.
 * This queue is used to hand off output records from the async thread pool to the
 * corresponding StreamThread to ensure that all further processing beyond/outside
 * the async processor is single threaded and executed by the original StreamThread.
 * <p>
 * Threading notes:
 * -Produces to queue --> async thread pool
 * -Consumes from queue --> StreamThread
 * -One per physical AsyncProcessor instance
 *   (ie per logical processor per partition per StreamThread)
 */
public class ForwardingQueue<KOut, VOut> {

  // Use ConcurrentLinkQueue since we just need basic read-write thread safety with
  // non-blocking, simple FIFO queue semantics
  private final Queue<ForwardableRecord<KOut, VOut>> forwardableRecords =
      new ConcurrentLinkedQueue<>();

  /**
   * @return true iff there are any records available to forward
   */
  public boolean isEmpty() {
    return forwardableRecords.isEmpty();
  }

  /**
   * Add a new record that is ready and able to be forwarded by the StreamThread
   * <p>
   * Should only be invoked by AsyncThreads
   */
  public void addToQueue(
      final Record<KOut, VOut> record,
      final String childName, // can be null
      final AsyncProcessorRecordContext recordContext,
      final Runnable forwardingListener
      ) {
    forwardableRecords.add(
        new ForwardableRecord<>(record, childName, recordContext, forwardingListener)
    );
  }

  /**
   * Add a new record that is ready and able to be forwarded by the StreamThread
   * <p>
   * Should only be invoked by StreamThreads
   */
  public ForwardableRecord<KOut, VOut> poll() {
    return forwardableRecords.poll();
  }

}