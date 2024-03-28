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

import dev.responsive.kafka.api.async.internals.AsyncThread;
import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;

/**
 * A simple wrapper layer that routes processor context accesses that occur during
 * the user's #process method when executed by a thread from the async pool to the
 * async thread's specific instance of the AsyncProcessorContext. Each execution thread
 * in the async pool gets its own async context and the mapping is final and immutable
 * once the async pool is initialized, allowing lock-free/contention-free routing and
 * processor context access.
 * <p>
 * This context router allows us to work around the change in execution thread that
 * occurs after an AsyncProcessor is initialized and processing begins (then flips
 * again when the processor is closed).
 * Since a processor context is only passed into a user's Processor implementation
 * once, during #init, they will need to save a reference to the exact context
 * object that's passed to #init, which is executed by the StreamThread. But after
 * that point, it will be only AsyncThreads executing the user's #process method,
 * and these async threads each have their individual context that is different
 * from the one belonging to the StreamThread (and to each other's). To make sure
 * any ProcessorContext calls the user makes inside #process reach the appropriate
 * context belonging to the currently executing thread, instead of an actual context,
 * we pass this "context router" in when invoking #init on the user's processor.
 * This way, when they save a reference to the context, it is not the StreamThread's
 * context but the router instead, and any calls made later in #process will be
 * routed accordingly.
 * <p>
 * The StreamThread will flip the router to "processing mode" after it returns from
 * the user's #init method. When the AsyncProcessor is eventually closed, the
 * StreamThread will turn processing mode back off just before invoking the #close
 * method of the user's processor.
 * When in "processing mode", the router expects only AsyncThread access and should
 * return only {@link AsyncThreadProcessorContext} instances.
 * When processing mode is off, the router expects the executing thread to be the
 * original StreamThread, and will only return the {@link StreamThreadProcessorContext}
 * that belongs to it.
 * <p>
 * Threading notes:
 * -Accessed by both the StreamThread and AsyncThreadPool, but only one or the other
 *  depending on whether "processing mode" is on or off.
 * -Although the async processing framework is responsible for setting up and managing
 *  the state of this class, the context router is not itself for use by the framework
 *  which should always have a handle on the specific context instance and pass things
 *  off directly between if needed. In other words, this class is simply made to act
 *  as an interface between the user and the underlying context objects. It is only
 *  passed in to the user's #init and only serves to redirect the ProcessorContext
 *  methods that the user invokes from their processor (whether in #init, #process,
 *  or #close)
 */
@SuppressWarnings("checkstyle:overloadmethodsdeclarationorder")
public class AsyncContextRouter<KOut, VOut>
    implements ProcessorContext<KOut, VOut>, FixedKeyProcessorContext<KOut, VOut> {

  private final Logger log;

  private final int partition;

  // Flipped to true once regular processing begins, ie between when #init and #close
  // are invoked on the AsyncProcessor.
  // When true, the StreamThread should be executing and its context used to delegate
  // When false, only AsyncThreads should be accessing this router
  private final AtomicBoolean isInProcessingMode = new AtomicBoolean(false);

  private final StreamThreadProcessorContext<KOut, VOut> streamThreadProcessorContext;

  public AsyncContextRouter(
      final String logPrefix,
      final int partition,
      final StreamThreadProcessorContext<KOut, VOut> streamThreadProcessorContext
  ) {
    this.log = new LogContext(logPrefix).logger(AsyncContextRouter.class);
    this.partition = partition;

    this.streamThreadProcessorContext = streamThreadProcessorContext;
  }

  public void startProcessingMode() {
    isInProcessingMode.setOpaque(true);
  }

  public void endProcessingMode() {
    isInProcessingMode.setOpaque(false);
  }

  /**
   * Look up the appropriate context based on whether processing mode is on
   */
  private ProcessorContext<KOut, VOut> lookupContext() {
    if (isInProcessingMode.getOpaque()) {
      return lookupContextForAsyncThread();
    } else {
      return streamThreadProcessorContext;
    }
  }

  /**
   * Look up the appropriate context based on whether processing mode is on,
   * and return it as a fixed-key context. This is needed because unfortunately
   * there is no ProcessorContext interface with all #forward overloads beneath
   * and so we have to split this method up into two versions, even though they
   * do the exact same thing inside.
   */
  private FixedKeyProcessorContext<KOut, VOut> lookupFixedKeyContext() {
    if (isInProcessingMode.getOpaque()) {
      return lookupContextForAsyncThread();
    } else {
      return streamThreadProcessorContext;
    }
  }

  /**
   * Look up the context when you know it should be an AsyncThreadProcessorContext.
   * This method includes a safety check to verify that assumption
   */
  private AsyncThreadProcessorContext<KOut, VOut> lookupContextForAsyncThread() {
    if (Thread.currentThread() instanceof AsyncThread) {
      return ((AsyncThread) Thread.currentThread()).context(partition);
    } else {
      log.error("Attempted to look up async processor context but is not executing "
                    + "on an AsyncThread");

      throw new IllegalStateException(
          "Cannot get an async processor context for a non-async thread"
      );
    }
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    lookupContext().forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final Record<K, V> record,
      final String childName
  ) {
    lookupContext().forward(record, childName);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final FixedKeyRecord<K, V> record
  ) {
    lookupFixedKeyContext().forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final FixedKeyRecord<K, V> record,
      final String childName
  ) {
    lookupFixedKeyContext().forward(record);
  }

  @Override
  public String applicationId() {
    return lookupContext().applicationId();
  }

  @Override
  public TaskId taskId() {
    return lookupContext().taskId();
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return lookupContext().recordMetadata();
  }

  @Override
  public Serde<?> keySerde() {
    return lookupContext().keySerde();
  }

  @Override
  public Serde<?> valueSerde() {
    return lookupContext().valueSerde();
  }

  @Override
  public File stateDir() {
    return lookupContext().stateDir();
  }

  @Override
  public StreamsMetrics metrics() {
    return lookupContext().metrics();
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    if (isInProcessingMode.getOpaque()) {
      log.error("Call to context#getStateStore after the processor was initialized");

      throw new IllegalArgumentException("Cannot call #getStateStore while in processing mode, "
                                             + "you must initialize all state stores during the"
                                             + "processor's #init method");
    }
    return streamThreadProcessorContext.getStateStore(name);
  }

  @Override
  public Cancellable schedule(
      final Duration interval,
      final PunctuationType type,
      final Punctuator callback
  ) {
    return lookupContext().schedule(interval, type, callback);
  }

  @Override
  public void commit() {
    lookupContext().commit();
  }

  @Override
  public Map<String, Object> appConfigs() {
    return lookupContext().appConfigs();
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(final String prefix) {
    return lookupContext().appConfigsWithPrefix(prefix);
  }

  @Override
  public long currentSystemTimeMs() {
    return lookupContext().currentSystemTimeMs();
  }

  @Override
  public long currentStreamTimeMs() {
    return lookupContext().currentStreamTimeMs();
  }

}
