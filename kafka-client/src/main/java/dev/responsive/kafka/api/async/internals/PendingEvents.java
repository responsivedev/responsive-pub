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

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A data structure for holding and tracking all pending events
 * for a given processor instance within each task.
 * <p>
 * This includes all async events from the time they are first created
 * up until they are finalized and transition to DONE. It also contains
 * failed events that will never be finalized, and holds them until the
 * event's error has been checked and processed, and handled.
 */
public class PendingEvents {

  private final Logger log;

  private final Set<AsyncEvent> pendingEvents = new HashSet<>();
  private final Map<AsyncEvent, Future<?>> scheduledEvents = new HashMap<>();
  private final Queue<AsyncEvent> failedEvents = new LinkedList<>();

  public PendingEvents(final String logPrefix) {
    this.log = new LogContext(logPrefix).logger(PendingEvents.class);
  }

  public boolean isEmpty() {
    return pendingEvents.isEmpty();
  }

  public int numPendingEvents() {
    return pendingEvents.size();
  }

  public void registerCreatedEvent(final AsyncEvent event) {
    if (pendingEvents.contains(event)) {
      log.error("Attempted to create new async event but it already exists");
      throw new IllegalStateException("Can't add new pending event due to event already present");
    }

    pendingEvents.add(event);
  }

  public void unregisterCompletedEvent(final AsyncEvent event) {
    if (scheduledEvents.containsKey(event)) {
      log.error("Attempted to finalize async event but it was still being scheduled");
      throw new IllegalStateException("Can't remove pending event due to still being scheduled");
    } else if (!pendingEvents.contains(event)) {
      log.error("Attempted to finish event but no such pending event was found");
      throw new IllegalStateException("Can't remove pending event due to event not found");
    }

    pendingEvents.remove(event);
  }

  public void schedulePendingEvent(final AsyncEvent event, final Future<?> future) {
    if (scheduledEvents.containsKey(event)) {
      log.error("Attempted to schedule async event but it was already scheduled");
      throw new IllegalStateException("Can't schedule event due to event already being scheduled");
    } else if (!pendingEvents.contains(event)) {
      log.error("Attempted to schedule event but no such pending event was found");
      throw new IllegalStateException("Can't schedule event due to pending event not found");
    }

    scheduledEvents.put(event, future);
  }

  public void removeScheduledEvent(final AsyncEvent event) {
    if (!scheduledEvents.containsKey(event)) {
      log.error("Attempted to remove scheduled async event but it was not being scheduled");
      throw new IllegalStateException("Can't unschedule event due to event not being scheduled");
    } else if (!pendingEvents.contains(event)) {
      log.error("Attempted to remove scheduled event but no such pending event was found");
      throw new IllegalStateException("Can't unschedule event due to pending event not found");
    }

    scheduledEvents.remove(event);
  }

  public void cancelScheduledEvents() {
    scheduledEvents.values().forEach(f -> f.cancel(false));
    scheduledEvents.clear();
  }

  /**
   * Register an event that has failed for any reason. Failed events will be held onto
   * until the owning processor can handle the exception, for example by shutting down
   * permanently upon a fatal error.
   */
  public void registerFailedEvent(final AsyncEvent event) {
    if (scheduledEvents.containsKey(event)) {
      log.error("Attempted to fail async event but it was still being scheduled");
      throw new IllegalStateException("Can't fail event due to event still being scheduled");
    } else if (!pendingEvents.contains(event)) {
      log.error("Attempted to schedule event but no such pending event was found");
      throw new IllegalStateException("Can't fail event due to pending event not found");
    }

    failedEvents.add(event);
  }

  /**
   * Poll and return the next failed event, if any. Returns null if there are no failed
   * events that have not yet been handled.
   */
  public AsyncEvent processFailedEvent() {
    final AsyncEvent failedEvent = failedEvents.poll();

    if (failedEvent != null) {
      if (!pendingEvents.contains(failedEvent)) {
        log.error("Attempted to process failed event but no such pending event was found");
        throw new IllegalStateException("Can't return failed event due to pending event not found");
      }
      pendingEvents.remove(failedEvent);
    }

    return failedEvent;
  }

}
