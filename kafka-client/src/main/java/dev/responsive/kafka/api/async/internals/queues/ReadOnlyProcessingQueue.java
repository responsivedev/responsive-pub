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

/**
 * An interface with only the read APIs of a {@link ProcessingQueue}, to ensure
 * that the AsyncThreads can only consume from this queue and not write to or
 * modify it in any way
 * See {@link ProcessingQueue} for more details on the queue semantics
 * <p>
 * Threading notes:
 * -these APIs are made available exclusively to the AsyncThreads
 */
public interface ReadOnlyProcessingQueue {

  /**
   * Blocking read API that returns the next available event to process
   * and waits for a new event to be scheduled if necessary. May return
   * null if the queue was closed while waiting for an event
   *
   * @return the next available event across all partitions,
   *         or null if the queue was closed before one was available
   */
  AsyncEvent take() throws InterruptedException;
}
