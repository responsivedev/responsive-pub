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

package dev.responsive.kafka.api.async.internals.contexts;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.events.DelayedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A specific variant of the async processor context to be used by the AsyncThread.
 * This context handles everything needed to execute the user's
 * {@link Processor#process} method asynchronously, such as preparing the metadata
 * and internal state to reflect what the record would have seen when it was first
 * sent to the AsyncProcessor by the StreamThread, and intercepting calls to
 * {@link ProcessorContext#forward(Record)} in order to hand them back to the original
 * StreamThread.
 * <p>
 * Threading notes:
 * -For use by AsyncThreads only
 * -One per AsyncThread per StreamThread per task
 */
public class AsyncThreadProcessorContext<KOut, VOut>
    extends AsyncProcessorContext<KOut, VOut> {

  // The AsyncEvent that is currently being processed by this AsyncThread. Updated each
  // time a new event is picked up from the processing queue but before beginning
  // to process it (ie invoking #process on the input record for this event), as
  // part of the preparation for each async process
  private AsyncEvent<?, ?> currentAsyncEvent;

  public AsyncThreadProcessorContext(
      final ProcessorContext<?, ?> delegate
  ) {
    super(delegate);
  }

  public void prepareToProcessNewEvent(final AsyncEvent<?, ?> nextEventToProcess) {
    currentAsyncEvent = nextEventToProcess;

    // TODO: should we (re)set any other metadata? For example should we
    //  return the original streamTime (which corresponds to the time when
    //  that record was actually picked up) or the current streamTime (which
    //  may be later that what it would have been with synchronous processing,
    //  but will be more aligned with the streamTime being tracked & used by the
    //  state store internals
    super.prepareForDelayedExecution(nextEventToProcess.recordContext());

    currentAsyncEvent.transitionToProcessing();
  }

  @SuppressWarnings("unchecked")
  public <KIn, VIn> AsyncEvent<KIn, VIn> currentAsyncEvent() {
    return (AsyncEvent<KIn, VIn>) currentAsyncEvent;
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    currentAsyncEvent.addForwardedRecord(new DelayedForward<>(record, null));
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final Record<K, V> record,
      final String childName
  ) {
    currentAsyncEvent.addForwardedRecord(new DelayedForward<>(record, childName));
  }
}
