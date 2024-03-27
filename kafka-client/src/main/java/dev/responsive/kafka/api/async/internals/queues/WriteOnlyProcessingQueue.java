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
import java.util.List;

/**
 * An interface with only the write APIs of a {@link ProcessingQueue}, to ensure
 * that the StreamThread is the only one that can write to or modify the
 * processing queue, and prevent the StreamThread from attempting to
 * consume events from the queue
 * See {@link ProcessingQueue} for more details on the queue semantics
 * <p>
 * Threading notes:
 * -these APIs are made available exclusively to the StreamThread
 */
public interface WriteOnlyProcessingQueue {

  /**
   * Register a new partition queue.
   * <p>
   * Threading notes:
   * -only invoked by StreamThread
   * -no locking/signalling required since partitions are stored in concurrent map
   *   and we don't add new records in this method, just a new/empty queue
   */
  void addPartition(final int partition);

  /**
   * Unregister an existing partition queue and remove any scheduled events
   * <p>
   * Threading notes:
   * -only invoked by StreamThread
   * -no locking/signalling required since partitions are stored in concurrent map
   * -removed partitions will be empty anyways, unless we hit an unclean shutdown
   */
  void removePartition(final int partition);

  /**
   * Non-blocking write API. Schedules a list of events for processing while
   * maintaining order within the associated partition
   */
  void offer(final int partition, final List<AsyncEvent> events);

  /**
   * Mark the queue as closed and notify any waiting threads.
   */
  void close();
}
