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

import dev.responsive.kafka.api.async.internals.AsyncKeyValueStore;
import dev.responsive.kafka.internal.stores.ResponsiveKeyValueStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A special version of the usual {@link ProcessorContext} for async processing. It's used
 * to intercept calls that need to be delayed/invoked on a specific thread, as well as to (re)set
 * metadata that's needed for record forwarding or may be accessed by a user within their #process
 * implementation. This enables delayed execution of {@link Processor#process} and
 * {@link ProcessorContext#forward}.
 *
 * <p>
 * This class must therefore wrap any/all instances of the original processor context which
 * means we have to make sure it can effectively mimic the actual {@link ProcessorContext} that is
 * used and expected both by Kafka Streams and the user's {@link Processor} implementation.
 * For this reason, we have to go a step further and actually implement the
 * {@link InternalProcessorContext} interface for internal Streams use, rather than just the public
 * {@link ProcessorContext} that's exposed to users, due to various casting and internal method
 * access that occurs in the processor context lifecycle. For example, we ourselves make this
 * cast in the #init method of our StateStore implementations (see
 * {@link ResponsiveKeyValueStore#init} for one such case)
 *
 * <p>
 * Threading notes:
 * -Exclusively owned/accessed by a single thread throughout its lifetime
 * -Each StreamThread and AsyncThread will have one
 * -One per physical AsyncProcessor instance per thread (StreamThread + all AsyncThreads in pool)
 *   (ie per logical processor per partition per thread)
 */
public abstract class AsyncProcessorContext<KOut, VOut>
    extends DelegatingInternalProcessorContext<KOut, VOut> {

  // We're being overly cautious here as the taskId is not actually mutated during
  // normal processing, but better safe than sorry (and to protect ourselves against future changes)
  private final TaskId taskId;

  private final Serde<?> keySerde;
  private final Serde<?> valueSerde;

  // is empty for records that originate from upstream punctuators rather than input topics
  private final Optional<RecordMetadata> recordMetadata;

  private final ProcessorNode<?, ?, ?, ?> asyncProcessorNode;

  @SuppressWarnings("unchecked")
  public AsyncProcessorContext(final ProcessorContext<?,?> delegate) {
    super((InternalProcessorContext<KOut, VOut>) delegate);

    taskId = super.taskId();
    keySerde = super.keySerde();
    valueSerde = super.valueSerde();

    recordMetadata = super.recordMetadata();
    asyncProcessorNode = super.currentNode();
  }

  /**
   * (Re)set all inner state and metadata to prepare for an async execution
   * such as processing input records or forwarding output records
   */
  protected void prepareForAsyncExecution(final ProcessorRecordContext recordContext) {
    super.setRecordContext(recordContext);
    super.setCurrentNode(asyncProcessorNode);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    throw new IllegalStateException("Must use a StreamThreadProcessorContext to execute forwards");
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record, final String childName) {
    throw new IllegalStateException("Must use a StreamThreadProcessorContext to execute forwards");
  }

  @Override
  public TaskId taskId() {
    return taskId;
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return recordMetadata;
  }

  @Override
  public Serde<?> keySerde() {
    return keySerde;
  }

  @Override
  public Serde<?> valueSerde() {
    return valueSerde;
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    // If this method is hit we can assume it was because the user attempted to invoke it during
    // their processor's #process method, instead of during #init. The StreamThreadProcessorContext,
    // which is the one used during #init, overrides this method and actually implements it
    throw new IllegalStateException("Must initialize state stores during the Processor's #init method");
  }

  @Override
  public long currentStreamTimeMs() {
    // TODO: The semantics here are up for debate, should we return the "true" stream-time
    //  at the point when {@link #currentStreamTimeMs} is invoked, or the "original" stream-time
    //  as of when the record was first passed to the processor?
    //  Right now we choose to delegate this since the "true" stream-time is the one that is
    //  generally going to match up with what the underlying store is seeing/tracking and making
    //  decisions based off, giving more internally consistent results.
    //  It's probably worth giving a bit more consideration to, however, and possibly
    //  even soliciting user feedback on
    return super.currentStreamTimeMs();
  }

  public String asyncProcessorName() {
    return asyncProcessorNode.name();
  }


}
