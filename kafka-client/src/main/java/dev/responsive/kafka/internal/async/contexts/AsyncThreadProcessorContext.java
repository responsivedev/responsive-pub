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

package dev.responsive.kafka.internal.async.contexts;

import dev.responsive.kafka.internal.async.events.AsyncEvent;
import dev.responsive.kafka.internal.async.events.DelayedForward;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * A special kind of mock/wrapper context to be used by the AsyncThread.
 * This context handles everything needed to execute the user's
 * {@link Processor#process} method asynchronously, such as preparing the metadata
 * and internal state to reflect what the record would have seen when it was first
 * sent to the AsyncProcessor by the StreamThread, and intercepting calls to
 * {@link ProcessorContext#forward(Record)} in order to hand them back to the original
 * StreamThread.
 * <p>
 * Besides intercepting calls to forward and preparing internal metadata, the other
 * important job of this specific context type is to protect the underlying context
 * from being mutated while the StreamThread is using it elsewhere in the topology,
 * and likewise to protect the users from the StreamThread's mutations so that any
 * metadata they access through public APIs reflects the async processor and not
 * whatever processor is being executed by the StreamThread with the "real" context.
 * <p>
 * While the StreamThread's async context enables delayed operations by
 * (re)setting any internal state of the underlying context, the AsyncThread's
 * context does the opposite and instead protects the underlying context from
 * being mutated.
 * <p>
 * Note: while semantically we could have this class extend the {@link DelegatingProcessorContext}
 * like the {@link StreamThreadProcessorContext} does, we intentionally implement only the
 * {@link MergedProcessorContext} instead for safety/compatibility reasons. This is to
 * make sure we catch any new additions to the public ProcessorContext interface, in
 * case we need to intercept them since it is essential that we protect the
 * underlying context when accessed through the async threads.
 * <p>
 * Threading notes:
 * -For use by AsyncThreads only
 * -One per AsyncThread per physical AsyncProcessor instance
 *   (ie one per AsyncThread per StreamThread per async processor per partition)
 *   Equivalently, one per AsyncThread for each "original" ProcessorContext in Streams
 */
public class AsyncThreadProcessorContext<KOut, VOut>
    extends DelegatingProcessorContext<KOut, VOut, InternalProcessorContext<KOut, VOut>> {

  // The AsyncEvent that is currently being processed by this AsyncThread. Updated each
  // time a new event is picked up from the processing queue but before beginning
  // to process it (ie invoking #process on the input record for this event), as
  // part of the preparation for each async process
  private final AsyncEvent currentAsyncEvent;

  // The actual context used by Kafka Streams which was originally passed
  // in to the async processor during init. This MUST be protected from
  // any mutations and should only be delegated to in pure getters that
  // access immutable fields (such as applicationId)
  private final InternalProcessorContext<KOut, VOut> taskContext;

  // TODO: we won't need to do this until we support async with the DSL and support
  //  the new windowed emit semantics specifically, which is the only thing using it,
  //  but at some point we may need to make a copy of the context's processorMetadata
  //  for each async event when it's created and then (re)set it here alongside the
  //  recordContext.
  //  This could have nontrivial overhead although it's possible we can get away with
  //  just saving a single long rather than copying an entire map. This feature needs
  //  further inspection but isn't supported by either the async framework or in
  //  Responsive in general, so it's not urgent.
  public AsyncThreadProcessorContext(
      final InternalProcessorContext<KOut, VOut> taskContext,
      final AsyncEvent currentAsyncEvent
  ) {
    this.taskContext = taskContext;
    this.currentAsyncEvent = currentAsyncEvent;
  }

  public AsyncEvent currentAsyncEvent() {
    return currentAsyncEvent;
  }

  private <K extends KOut, V extends VOut> void interceptForward(
      final DelayedForward<K, V> interceptedForward
  ) {
    currentAsyncEvent.addForwardedRecord(interceptedForward);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final Record<K, V> record
  ) {
    interceptForward(DelayedForward.ofRecord(record, null));
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final Record<K, V> record,
      final String childName
  ) {
    interceptForward(DelayedForward.ofRecord(record, childName));
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final FixedKeyRecord<K, V> record
  ) {
    interceptForward(DelayedForward.ofFixedKeyRecord(record, null));
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final FixedKeyRecord<K, V> record,
      final String childName
  ) {
    interceptForward(DelayedForward.ofFixedKeyRecord(record, childName));
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return Optional.ofNullable(currentAsyncEvent.recordContext());
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    // If this method is hit we can assume the user invoked it from their
    // #process method, instead of during #init as intended, since this context
    // type is only accessed by AsyncThreads which only invoke #process
    throw new UnsupportedOperationException(
        "Must call #getStateStore during the Processor's #init method"
    );
  }

  @Override
  public Cancellable schedule(
      final Duration interval,
      final PunctuationType type,
      final Punctuator callback
  ) {
    throw new UnsupportedOperationException("Please initialize any punctuations during #init");
  }

  @Override
  public void commit() {
    // This is technically not thread-safe since it sets a simple (non-volatile) commitRequested
    // flag in the StreamTask, but since a commit request is only best-effort and there's no
    // guarantee of when the commit will actually occur, we aren't technically violating
    // the semantics here.
    // We can support this for real by using a simple AtomicBoolean to flag commit requests
    // and signal the StreamThread to delegate the request down to the original context
    // when it next re-enters the async processor. But that feels like overkill
    taskContext.commit();
  }

  @Override
  public long currentSystemTimeMs() {
    // TODO: It probably makes more sense to return the StreamThread's current view
    //  of system time here, rather than the system time when the record was first
    //  picked up, but it's not thread-safe to delegate to the original context and
    //  retrieving the actual system time has been known to significantly impact
    //  performance when performed on each invocation of #process
    return currentAsyncEvent.systemTime();
  }

  @Override
  public long currentStreamTimeMs() {
    // TODO: The semantics here are up for debate, should we return the "true" stream-time
    //  at the point when {@link #currentStreamTimeMs} is invoked, or the "original" stream-time
    //  as of when the record was first passed to the processor?
    //  For now we just return the stream-time as of when the record was picked up, mainly
    //  because there's no concurrency control around the stream-time in Streams
    //  However, it might make more sense to try and return the actual, latest stream-time
    //  since this will be more in line with the stream-time as seen/used by our internal
    //  state stores. If we decide to go this route, we can get the stream-time from our
    //  own stores to avoid the thread-safety concerns with delegating to the original context
    //  I'm not sure that many people use this API so let's just see if the semantics hold
    return currentAsyncEvent.streamTime();
  }

  @Override
  public InternalProcessorContext<KOut, VOut> delegate() {
    return taskContext;
  }
}
