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
 * A write-only version of the {@link FinalizingQueue} intended for the
 * AsyncThread(s) only.
 * <p>
 * This queue simply facilitates the handover of processed events from the
 * AsyncThread back to the StreamThread for finalization.
 * <p>
 * See {@link FinalizingQueue} for full javadocs, and
 * {@link ReadOnlyFinalizingQueue} for the read-only version of this queue
 */
public interface WriteOnlyFinalizingQueue {

  /**
   * Schedules a record that the AsyncThread finished processing and inserts it
   * into the queue for the StreamThread to pick up and finalize
   */
  void scheduleForFinalization(final AsyncEvent processedEvent);

  /**
   * Schedules a record that the AsyncThread failed to process and inserts it
   * into the queue for the StreamThread to pick up and finalize
   */
  void scheduleFailedForFinalization(
      final AsyncEvent processedEvent,
      final RuntimeException exception
  );
}
