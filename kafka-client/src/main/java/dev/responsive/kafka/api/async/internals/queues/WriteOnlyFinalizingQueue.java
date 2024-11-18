/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
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
   * Adds a record that the AsyncThread finished processing successfully
   * and inserts it into the back of the queue for the StreamThread to pick up
   * and finalize.
   */
  void addFinalizableEvent(final AsyncEvent processedEvent);

  /**
   * Adds a record that the AsyncThread failed to process successfully
   * and inserts it into the front of the queue for the StreamThread to pick up
   * and handle.
   */
  void addFailedEvent(
      final AsyncEvent processedEvent,
      final Throwable throwable
  );
}
