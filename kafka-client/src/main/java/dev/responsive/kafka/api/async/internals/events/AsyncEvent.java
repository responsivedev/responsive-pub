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

package dev.responsive.kafka.api.async.internals.events;

import static dev.responsive.kafka.api.async.internals.AsyncUtils.processorRecordContextHashCode;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.slf4j.Logger;

/**
 * An "async event" describes the full end-to-end processing of a single input record.
 * This includes everything that needs to happen for a record to be considered
 * "fully processed" meaning it is safe to commit the corresponding offset.
 * The event encapsulates the input record itself and its corresponding record
 * context and metadata, as well any "outputs" that it triggers, such as forwarded
 * records or writes to a state store.
 * <p>
 * An async event has several stages:
 * 0. SCHEDULING: A new event begins when an input record is passed into
 *    the {@link AsyncProcessor#process} method by the StreamThread.
 *    During this stage, it waits in the SchedulingQueue until the StreamThread
 *    determines that it can be safely processed (ie all previous async events with
 *    the same input record key have completed the final stage and are finished.)
 * 1. INPUT_READY: An event has been "scheduled" and transitions to the INPUT_READY
 *    stage when the StreamThread pulls it from the scheduling queue and adds it to
 *    the processing queue. This is the first time an event transfers ownership between
 *    threads, with the StreamThread handing off the event to the AsyncThread here.
 *    Any event in this stage is ready to be processed and will do so when the next
 *    AsyncThread becomes available. Events spend their time in this stage waiting
 *    in the processing queue.
 * 2. PROCESSING: Events transition to PROCESSING when they leave the processing queue
 *    An AsyncThread that picks up an event from the processing queue will prepare
 *    itself for the new event by resetting various internal state and updating some
 *    fields of the {@link AsyncThreadProcessorContext} to match the metadata from
 *    that event's record context. After this, the AsyncThread will go ahead with
 *    the processing by invoking the user's #process on the input record for this event.
 *    During this processing, any number of output records of two types can be triggered
 *    by the user's processor: forwarding records to the context, or issuing writes to
 *    a state store. Both types of output record are routed back to the event which
 *    handles getting them into the corresponding queue -- ie the forwarding queue or
 *    writing queue -- and thus make them accessible to the StreamThread for their
 *    eventual execution. These outputs records are handed off to the StreamThread at
 *    any point during the user's #process implementation, but the event will remain
 *    in the PROCESSING stage until the user's #process method returns. This stage
 *    marks the last time the input record is handled by the processing framework,
 *    and the event will turn its focus on the output records from this point on.
 * 3. OUTPUT_READY: After processing the input record and registering any output records
 *    by sending them to the corresponding queue, the async event itself will be passed
 *    from the AsyncThread back to the original StreamThread, by placing it into the
 *    finalizing queue. Events will remain in this queue while they wait for the
 *    StreamThread to become available and pull them from the finalizing queue.
 * 3. FINALIZING: The last stage before completion, an event transitions to
 *    this when the StreamThread polls it from the finalizing queue and executes
 *    any output records that were intercepted during the PROCESSING phase. The
 *    event waits in this stage while any forwards and writes are performed on
 *    the original StreamThread.
 * 4. DONE: Once all input and output records of the event have been fully
 *    processed, the StreamThread will mark the event as completed, at which
 *    point the event itself is considered DONE and Streams can safely commit
 *    the corresponding offset. The StreamThread will close out the event by
 *    signalling to the scheduling queue that it can unblock the next event
 *    with the same input record key, if one arrived after this event and was
 *    waiting in the scheduling queue for the earlier event to complete.
 * <p>
 * An AsyncEvent is essentially parameterized by the input record, using the input
 * key to determine which events are blocked on another. Each async event can have
 * zero, one, or multiple of each output record type: forwards and writes. However
 * it is always the input record that determines the characteristics and ordering of
 * the overall event
 */
public class AsyncEvent {

  // State machine for an async event lifecycle. Every event must pass through each
  // state exactly once and progress these following the same order in which they
  // are defined below
  public enum State {
    SCHEDULING,
    TO_PROCESS,
    PROCESSING,
    TO_FINALIZE,
    FINALIZING,
    DONE
  }

  private final Logger log;

  private State currentState;

  private final Object inputRecordKey;
  private final int partition;
  private final long systemTime;
  private final long streamTime;

  private final ProcessorRecordContext recordContext; // may be null if punctuator-created

  private final Runnable processInputRecord;

  private final Queue<DelayedForward<?, ?>> outputForwards = new LinkedList<>();
  private final Queue<DelayedWrite<?, ?>> outputWrites = new LinkedList<>();

  public AsyncEvent(
      final String logPrefix,
      final Record<?, ?> inputRecord,
      final int partition,
      final ProcessorRecordContext recordContext,
      final long currentStreamTime,
      final long currentSystemTime,
      final Runnable processInputRecord
  ) {
    this.currentState = State.SCHEDULING;
    this.inputRecordKey = inputRecord.key();
    this.partition = partition;
    this.streamTime = currentStreamTime;
    this.systemTime = currentSystemTime;
    this.recordContext = recordContext;
    this.processInputRecord = processInputRecord;

    if (recordContext == null) {
      this.log = new LogContext(logPrefix).logger(AsyncEvent.class);
    } else {
      this.log = new LogContext(String.format(
          "%s <%d> ", logPrefix, recordContext.offset()
      )).logger(AsyncEvent.class);
    }
  }

  public AsyncEvent(
      final String logPrefix,
      final FixedKeyRecord<?, ?> fixedKeyInputRecord,
      final int partition,
      final ProcessorRecordContext recordContext,
      final long currentStreamTime,
      final long currentSystemTime,
      final Runnable processInputRecord
  ) {
    this.currentState = State.SCHEDULING;
    this.inputRecordKey = fixedKeyInputRecord.key();
    this.partition = partition;
    this.streamTime = currentStreamTime;
    this.systemTime = currentSystemTime;
    this.recordContext = recordContext;
    this.processInputRecord = processInputRecord;

    if (recordContext == null) {
      this.log = new LogContext(logPrefix).logger(AsyncEvent.class);
    } else {
      this.log = new LogContext(String.format(
          "%s <%d> ", logPrefix, recordContext.offset()
      )).logger(AsyncEvent.class);
    }
  }

  public Runnable inputRecordProcessor() {
    return processInputRecord;
  }

  public void addForwardedRecord(final DelayedForward<?, ?> delayedForward) {
    outputForwards.add(delayedForward);
  }

  public void addWrittenRecord(final DelayedWrite<?, ?> delayedWrite) {
    outputWrites.add(delayedWrite);
  }

  @SuppressWarnings("unchecked")
  public <KOut, VOut> DelayedForward<KOut, VOut> nextForward() {
    return (DelayedForward<KOut, VOut>) outputForwards.poll();
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> DelayedWrite<KS, VS> nextWrite() {
    return (DelayedWrite<KS, VS>) outputWrites.poll();
  }

  public State currentState() {
    return currentState;
  }

  public void transitionToToProcess() {
    if (!currentState.equals(State.SCHEDULING)) {
      log.error("[{}] Attempted to mark an async event as ready for processing but the event was "
                    + "not in the SCHEDULING state", currentState.name());
      throw new IllegalStateException("Cannot transition to AWAITING_PROCESS from the state "
                                          + currentState.name());
    }
    currentState = State.TO_PROCESS;
  }

  public void transitionToProcessing() {
    if (!currentState.equals(State.TO_PROCESS)) {
      log.error("[{}] Attempted to mark an async event as being processed but the event was "
                    + "not in the TO_PROCESS state", currentState.name());
      throw new IllegalStateException("Cannot transition to PROCESSING from the state "
                                          + currentState.name());
    }
    currentState = State.PROCESSING;
  }

  public void transitionToToFinalize() {
    if (!currentState.equals(State.PROCESSING)) {
      log.error("[{}] Attempted to mark an async event as ready for finalization but "
                    + "the event was not in the PROCESSING state", currentState.name());
      throw new IllegalStateException("Cannot transition to TO_FINALIZE from the state "
                                          + currentState.name());
    }
    currentState = State.TO_FINALIZE;
  }

  public void transitionToFinalizing() {
    if (!currentState.equals(State.TO_FINALIZE)) {
      log.error("[{}] Attempted to mark an async event as finalizing but the event was not "
                    + "in the AWAITING_FINALIZE state", currentState.name());
      throw new IllegalStateException("Cannot transition to FINALIZING from the state "
                                          + currentState.name());
    }
    currentState = State.FINALIZING;
  }

  public void transitionToDone() {
    if (!currentState.equals(State.FINALIZING)) {
      log.error(
          "[{}] Attempted to mark an async event as DONE but the event not "
              + "in the FINALIZING state",
          currentState.name());
      throw new IllegalStateException(
          "Cannot transition to DONE from the state " + currentState.name());
    } else if (!(outputForwards.isEmpty() && outputWrites.isEmpty())) {
      log.error(
          "[{}] Attempted to mark an async event as complete without draining all output queues"
              + "first. Remaining forwards={} and remaining writes={}",
          currentState.name(), outputForwards.size(), outputWrites.size());
      throw new IllegalStateException(
          "Can't transition to DONE when there are still records in the output buffers");
    }

    currentState = State.DONE;
  }

  public ProcessorRecordContext recordContext() {
    return recordContext;
  }

  public long streamTime() {
    return streamTime;
  }

  public long systemTime() {
    return systemTime;
  }

  public int partition() {
    return partition;
  }

  @SuppressWarnings("unchecked")
  public <KIn> KIn inputKey() {
    return (KIn) inputRecordKey;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AsyncEvent that = (AsyncEvent) o;

    if (partition != that.partition) {
      return false;
    }
    if (systemTime != that.systemTime) {
      return false;
    }
    if (streamTime != that.streamTime) {
      return false;
    }
    if (!inputRecordKey.equals(that.inputRecordKey)) {
      return false;
    }
    return Objects.equals(recordContext, that.recordContext);
  }

  @Override
  public int hashCode() {
    int result = inputRecordKey.hashCode();
    result = 31 * result + partition;
    result = 31 * result + (int) (systemTime ^ (systemTime >>> 32));
    result = 31 * result + (int) (streamTime ^ (streamTime >>> 32));
    result = 31 * result
        + (recordContext != null ? processorRecordContextHashCode(recordContext, false) : 0);
    return result;
  }
}
