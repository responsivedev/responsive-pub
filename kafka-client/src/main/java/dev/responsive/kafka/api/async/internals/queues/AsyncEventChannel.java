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

import dev.responsive.kafka.api.async.internals.records.AsyncEvent;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The building block for an "async channel" which is used to pass records from
 * one thread type to another and supports blocking semantics.
 * TODO: implement proper interrupt semantics for clean closing
 */
public class AsyncEventChannel<KIn, VIn> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncEventChannel.class);

  private final BlockingQueue<AsyncEvent<KIn, VIn>> asyncEvents;

  public AsyncEventChannel() {
      this.asyncEvents = new LinkedBlockingQueue<>();
  }

  /**
   * A non-blocking read
   *
   * @return the next available event, or {@code null} if the queue is empty
   */
  public AsyncEvent<KIn, VIn> poll() {
    return asyncEvents.poll();
  }

  /**
   * A blocking read
   *
   * @return the next available event if one exists, otherwise blocks until a new event
   *         is added to the queue or the channel is closed.
   */
  public AsyncEvent<KIn, VIn> take() {
    try {
      return asyncEvents.take();
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      LOG.info("Interrupted while waiting to poll record");
      throw new RuntimeException("Interrupted during blocking poll");
    }
  }

  public void put(final AsyncEvent<KIn, VIn> event) {
    try {
      asyncEvents.put(event);
    } catch (final InterruptedException e) {
      // Just throw an exception for now to ensure shutdown occurs
      LOG.info("Interrupted while attempting to insert record");
      throw new RuntimeException("Interrupted during blocking put");
    }
  }

  public boolean isEmpty() {
    return asyncEvents.isEmpty();
  }

}
