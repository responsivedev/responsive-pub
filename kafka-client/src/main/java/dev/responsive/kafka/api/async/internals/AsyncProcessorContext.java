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

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

public class AsyncProcessorContext<KOut, VOut> implements ProcessorContext<KOut, VOut> {

  private final InternalProcessorContext<KOut, VOut> delegate;
  private final Map<String, AsyncKeyValueStore<?, ?>> stateStores = new HashMap<>();

  // We're being overly cautious here as the taskId is not actually mutated during
  // normal processing, but better safe than sorry (and to protect ourselves against future changes)
  private final TaskId taskId;

  private final Serde<?> keySerde;
  private final Serde<?> valueSerde;

  // is empty for records that originate from upstream punctuators rather than input topics
  private final ProcessorNode<?, ?, ?, ?> asyncProcessorNode;
  private final Optional<RecordMetadata> recordMetadata;

  @SuppressWarnings("unchecked")
  public AsyncProcessorContext(final ProcessorContext<?,?> delegate) {
    this.delegate = (InternalProcessorContext<KOut, VOut>) delegate;

    taskId = delegate.taskId();
    keySerde = delegate.keySerde();
    valueSerde = delegate.valueSerde();

    recordMetadata = delegate.recordMetadata();
    asyncProcessorNode = this.delegate.currentNode();
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    // TODO -- how do we make sure the delegate is "set" correctly? need to ensure things like
    //  #recordMetadata and currentNode (maybe others) correspond to the original processor state
    //  since the delegate will be mutated during normal processing flow
    //  Need to set: currentNode, recordContext
    delegate.forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record, final String childName) {
    // TODO -- see above comment
    delegate.forward(record, childName);
  }

  @Override
  public String applicationId() {
    return delegate.applicationId();
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
  public File stateDir() {
    return delegate.stateDir();
  }

  @Override
  public StreamsMetricsImpl metrics() {
    return delegate.metrics();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S extends StateStore> S getStateStore(final String name) {
    // TODO:
    //  1) We should be enforcing that this is called exactly once per store and only from #init
    //  This delegate call relies on mutable internal context fields and hence should never
    //  be called from the processors' #process method. This is good practice in vanilla Streams
    //  anyway, but it's especially important for the async processor
    //  2) Implement async StateStore for window and session stores
    final S userDelegate = delegate.getStateStore(name);
    if (userDelegate instanceof KeyValueStore) {
      final var asyncStore = new AsyncKeyValueStore<>((KeyValueStore<?, ?>) userDelegate, name);
      stateStores.put(name, asyncStore);
      return (S) asyncStore;
    } else {
      throw new UnsupportedOperationException(
          "Window and Session stores are not yet supported with async processing");
    }
  }

  @Override
  public Cancellable schedule(
      final Duration interval,
      final PunctuationType type,
      final Punctuator callback
  ) {
    throw new UnsupportedOperationException("Punctuators not yet supported for async processors");
  }

  @Override
  public void commit() {
    // this is only a best-effort request and not a guarantee, so it's fine to potentially delay
    delegate.commit();
  }

  @Override
  public Map<String, Object> appConfigs() {
    return delegate.appConfigs();
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(final String prefix) {
    return delegate.appConfigsWithPrefix(prefix);
  }

  @Override
  public long currentSystemTimeMs() {
    // TODO: assess how/when this is actually used in custom processors, and whether we should
    //  simply pass the new/true system time, or save the system time on initial invocation
    return delegate.currentSystemTimeMs();
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
    return delegate.currentStreamTimeMs();
  }

  /**
   * (Re)set all inner state and metadata to prepare for an async execution of #process
   */
  public void prepareForProcess(final ProcessorRecordContext recordContext) {
    delegate.setRecordContext(recordContext);
    delegate.setCurrentNode(asyncProcessorNode);
  }

  public ProcessorRecordContext recordContext() {
    return delegate.recordContext();
  }

  public ProcessorNode<?, ?, ?, ?> currentNode() {
    return delegate.currentNode();
  }

  public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
    // TODO -- can we use this to our advantage somehow?
  }

  public int partition() {
    return taskId().partition();
  }

  public long offset() {
    // For now we assume that (a) the record metadata exists, and (b) there is only one input topic
    // This allows us to assert that each input record can be uniquely identified -- and ordered --
    // according to their offset alone
    // These limitations can/should eventually be lifted, but for now are documented in the
    // javadocs for AsyncProcessorSupplier
    final Optional<RecordMetadata> metadata = delegate.recordMetadata();
    if (metadata.isEmpty()) {
      throw new UnsupportedOperationException("Record metadata was missing, which implies this "
                                                  + "record originated from a punctuator. Punctuator-produced records are not yet "
                                                  + "compatible with async processing.");
    }
    return metadata.get().offset();
  }

  public String currentProcessorName() {
    return delegate.currentNode().name();
  }

  public AsyncKeyValueStore<?, ?> getAsyncStore(final String storeName) {
    return stateStores.get(storeName);
  }

  public Map<String, AsyncKeyValueStore<?, ?>> getAllAsyncStores() {
    return stateStores;
  }

}
