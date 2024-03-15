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

package dev.responsive.kafka.api.async.internals.records;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import dev.responsive.kafka.api.async.internals.contexts.AsyncThreadProcessorContext;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
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
public class AsyncEvent<KIn, VIn> {

  // State machine for an async event lifecycle. Every event must pass through each
  // state exactly once and progress these following the same order in which they
  // are defined below
  public enum State {
    SCHEDULING,
    INPUT_READY,
    PROCESSING,
    OUTPUT_READY,
    FINALIZING,
    DONE
  }

  private final Logger log;

  private State currentState;

  private final Record<KIn, VIn> inputRecord;
  private final ProcessorRecordContext recordContext;
  private final Runnable processInputRecord;

  private final Queue<ForwardableRecord<?, ?>> outputForwards = new LinkedList<>();
  private final Queue<WriteableRecord<?, ?>> outputWrites = new LinkedList<>();

  public AsyncEvent(
      final String asyncProcessorName,
      final Record<KIn, VIn> inputRecord,
      final ProcessorRecordContext recordContext,
      final Runnable processInputRecord
  ) {
    this.currentState = State.SCHEDULING;
    this.log = new LogContext(String.format(
        "async-event [%s-%d] %s[%d]",
        asyncProcessorName, recordContext.partition(), recordContext.topic(), recordContext.offset()
    )).logger(AsyncEvent.class);

    this.inputRecord = inputRecord;
    this.recordContext = recordContext;
    this.processInputRecord = processInputRecord;
  }

  public Record<KIn, VIn> inputRecord() {
    return inputRecord;
  }

  public Runnable processInputRecord() {
    return processInputRecord;
  }

  @SuppressWarnings("unchecked")
  public <KOut, VOut> ForwardableRecord<KOut, VOut> nextForward() {
    return (ForwardableRecord<KOut, VOut>) outputForwards.poll();
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> WriteableRecord<KS, VS> nextWrite() {
    return (WriteableRecord<KS, VS>) outputWrites.poll();
  }

  public boolean isDone() {
    return currentState.equals(State.DONE);
  }

  public void transitionToInputReady() {
    if (!currentState.equals(State.SCHEDULING)) {
      log.error("[{}] Attempted to mark an async event as ready for processing but the event was "
                    + "not in the SCHEDULING state", currentState.name());
      throw new IllegalStateException("Cannot transition to INPUT_READY from the state "
                                          + currentState.name());
    }
    currentState = State.INPUT_READY;
  }

  public void transitionToOutputReady() {
    if (!currentState.equals(State.PROCESSING)) {
      log.error("[{}] Attempted to mark an async event as ready for output processing but "
                    + "the event was not in the PROCESSING state", currentState.name());
      throw new IllegalStateException("Cannot transition to OUTPUT_READY from the state "
                                          + currentState.name());
    }
    currentState = State.OUTPUT_READY;
  }

  public void transitionToFinalizing() {
    if (!currentState.equals(State.OUTPUT_READY)) {
      log.error("[{}] Attempted to mark an async event as finalizing but the event was not "
                    + "in the OUTPUT_READY state", currentState.name());
      throw new IllegalStateException("Cannot transition to FINALIZING from the state "
                                          + currentState.name());
    }
    currentState = State.FINALIZING;
  }

  public void transitionToDone() {
    if (!(outputForwards.isEmpty() && outputWrites.isEmpty())) {
      log.error("[{}] Attempted to mark an async event as complete without draining all output queues"
                    + "first. Remaining forwards={} and remaining writes={}",
                currentState.name(), outputForwards.size(), outputWrites.size());
      throw new IllegalStateException("Can't transition to DONE when there are still records "
                                          + "in the output buffers");
    } else if (!currentState.equals(State.FINALIZING)) {
      log.error("[{}] Attempted to mark an async event as DONE but the event not "
                    + "in the FINALIZING state", currentState.name());
      throw new IllegalStateException("Cannot transition to DONE from the state "
                                          + currentState.name());
    }
    currentState = State.DONE;
  }

  public ProcessorRecordContext recordContext() {
    return recordContext;
  }

  public KIn key() {
    return inputRecord.key();
  }

}
