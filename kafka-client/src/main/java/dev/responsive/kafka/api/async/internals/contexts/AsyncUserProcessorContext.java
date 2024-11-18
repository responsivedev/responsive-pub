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

package dev.responsive.kafka.api.async.internals.contexts;

import static dev.responsive.kafka.api.async.internals.AsyncUtils.isAsyncThread;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.isStreamThread;
import static dev.responsive.kafka.api.async.internals.AsyncUtils.isStreamThreadOrAsyncThread;

import dev.responsive.kafka.api.async.internals.events.DelayedWrite;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.slf4j.Logger;

/**
 * tl;dr
 * This class acts as a router to make sure that the correct thread's context is
 * returned whenever the user attempts to access the context from their processor,
 * whether during #process (in which case we serve an {@link AsyncThreadProcessorContext})
 * or during #init or #close (in which case we serve the {@link StreamThreadProcessorContext})
 * This class makes use of {@link ThreadLocal} to map threads to their contexts, and
 * passes the thread-specific local context as the delegate for the
 * {@link DelegatingProcessorContext}
 * <p>
 * More details:
 * This context router allows us to work around the change in execution thread that
 * occurs after an AsyncProcessor is initialized and processing begins (then flips
 * again when the processor is closed).
 * Since a processor context is only passed into a user's Processor implementation
 * once, during #init, they will need to save a reference to the exact context
 * object that's passed to #init, which is executed by the StreamThread. But after
 * that point, it will be only async threads executing the user's #process method,
 * and these async threads each have their individual context that is different
 * from the one belonging to the StreamThread (and to each other's). To make sure
 * any ProcessorContext calls the user makes inside #process reach the appropriate
 * context belonging to the currently executing thread, we pass this "context router"
 * in when invoking #init on the user's processor.
 * This way, when they save a reference to the context, it is not the StreamThread's
 * context but the router instead, and any calls made later in #process will be
 * routed accordingly.
 * <p>
 * Threading notes:
 * -Accessed by both the StreamThread and AsyncThreadPool.
 * -Although the async processing framework is responsible for setting up and managing
 *  the state of this class, the context router is not itself for use by the framework
 *  which should always have a handle on the specific context instance and pass things
 *  off directly between if needed. In other words, this class is simply made to act
 *  as an interface between the user and the underlying context objects. It is only
 *  passed in to the user's #init and only serves to redirect the ProcessorContext
 *  methods that the user invokes from their processor (whether in #init, #process,
 *  or #close)
 */
public class AsyncUserProcessorContext<KOut, VOut>
    extends DelegatingProcessorContext<KOut, VOut, MergedProcessorContext<KOut, VOut>>
    implements DelayedAsyncStoreWriter {

  private final Logger log;
  private final String streamThreadName;
  private final ProcessingContext taskContext;

  private final ThreadLocal<MergedProcessorContext<KOut, VOut>> threadLocalDelegate =
      new ThreadLocal<>();

  public AsyncUserProcessorContext(
      final String streamThreadName,
      final ProcessingContext taskContext,
      final String logPrefix
  ) {
    this.streamThreadName = streamThreadName;
    this.taskContext = taskContext;
    this.log = new LogContext(logPrefix).logger(AsyncUserProcessorContext.class);
  }

  /**
   * Getter for the "task context", ie the underlying context created by the StreamThread
   * when it created the corresponding StreamTask and owned by Kafka Streams, not the
   * async processing framework.
   */
  public ProcessingContext taskContext() {
    return taskContext;
  }

  @Override
  public MergedProcessorContext<KOut, VOut> delegate() {
    final var threadLocalContext = threadLocalDelegate.get();

    // Unfortunately the ThreadLocal's #isPresent method is only package-private
    if (threadLocalContext == null) {
      final String threadName = Thread.currentThread().getName();
      if (isStreamThreadOrAsyncThread(threadName, streamThreadName)) {
        log.error("Thread {} attempted to access the context but it was not initialized",
                  threadName);
        throw new IllegalStateException("Uninitialized thread requested context access");
      } else {
        log.error("Unexpected thread type attempted to access the context. Thread name: {}",
                  threadName);
        throw new UnsupportedOperationException("Illegal external thread requested context access");
      }
    }

    return threadLocalContext;
  }

  public void setDelegateForStreamThread(final StreamThreadProcessorContext<KOut, VOut> delegate) {
    final String threadName = Thread.currentThread().getName();
    log.debug("Initializing thread-local context for StreamThread {}", threadName);

    if (!isStreamThread(threadName, streamThreadName)) {
      log.error("Attempted to set StreamThread context but thread name was {}", threadName);
      throw new IllegalStateException("Incorrect thread initializing StreamThread context");
    }
    threadLocalDelegate.set(delegate);
  }

  public void setDelegateForAsyncThread(final AsyncThreadProcessorContext<KOut, VOut> delegate) {
    final String threadName = Thread.currentThread().getName();

    // log this one at TRACE since it's per-record unlike the StreamThread context
    log.trace("Initializing thread-local context for AsyncThread {}", threadName);

    if (!isAsyncThread(threadName, streamThreadName)) {
      log.error("Attempted to set AsyncThread context but thread name was {}", threadName);
      throw new IllegalStateException("Incorrect thread initializing AsyncThread context");
    }
    threadLocalDelegate.set(delegate);
  }

  @Override
  public void acceptDelayedWriteToAsyncStore(final DelayedWrite<?, ?> write) {
    final String threadName = Thread.currentThread().getName();
    if (!isAsyncThread(threadName, streamThreadName)) {
      log.error("A non async thread invoked put on an async store. Caller thread name was {}",
          threadName);
      // The most common reason this case might be hit, besides a bug in the async framework itself,
      // is if users attempt to insert into a state store from other Processor methods, like
      // #init or #close
      throw new IllegalStateException("Can only call #put on an async state store inside the "
          + "#process method of an async processor");
    }
    final AsyncThreadProcessorContext<KOut, VOut> asyncThreadContext
        = (AsyncThreadProcessorContext<KOut, VOut>) threadLocalDelegate.get();
    asyncThreadContext.currentAsyncEvent().addWrittenRecord(write);
  }
}
