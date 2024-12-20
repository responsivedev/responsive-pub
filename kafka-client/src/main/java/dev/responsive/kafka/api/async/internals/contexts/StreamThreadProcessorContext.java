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

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import dev.responsive.kafka.api.async.internals.events.AsyncEvent.State;
import dev.responsive.kafka.api.async.internals.events.DelayedForward;
import dev.responsive.kafka.api.async.internals.events.DelayedWrite;
import dev.responsive.kafka.api.async.internals.stores.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.internals.stores.AsyncStateStore;
import dev.responsive.kafka.api.async.internals.stores.AsyncTimestampedKeyValueStore;
import dev.responsive.kafka.api.async.internals.stores.AsyncTimestampedWindowStore;
import dev.responsive.kafka.api.async.internals.stores.AsyncWindowStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;

/**
 * A wrapper around the original processor context to be used by the StreamThread.
 * This context handles everything related to initialization of the processor
 * (eg {@link #getStateStore(String)}) as well as the "finalization" of async events,
 * ie preparing for (and executing) delayed forwards or writes.
 * Delayed forwards are intercepted by the other kind of async context, which is
 * unique to a given AsyncThread, called the {@link AsyncThreadProcessorContext}
 * <p>
 * Threading notes:
 * -For use by StreamThreads only
 * -One per physical AsyncProcessor
 *  (ie one per async processor per partition per StreamThread)
 */
public class StreamThreadProcessorContext<KOut, VOut>
    extends DelegatingProcessorContext<KOut, VOut, InternalProcessorContext<KOut, VOut>> {

  private final Logger log;

  private final Map<String, AsyncStateStore<?, ?>> allStoreNameToAsyncStore = new HashMap<>();
  private final Map<String, AsyncKeyValueStore<?, ?>> kvStoreNameToAsyncStore = new HashMap<>();
  private final Map<String, AsyncWindowStore<?, ?>> windowStoreNameToAsyncStore = new HashMap<>();

  private final ProcessorNode<?, ?, ?, ?> asyncProcessorNode;
  private final InternalProcessorContext<KOut, VOut> originalContext;
  private final DelayedAsyncStoreWriter delayedStoreWriter;

  public StreamThreadProcessorContext(
      final String logPrefix,
      final InternalProcessorContext<KOut, VOut> originalContext,
      final DelayedAsyncStoreWriter delayedStoreWriter
  ) {
    super();
    this.log = new LogContext(Objects.requireNonNull(logPrefix))
        .logger(StreamThreadProcessorContext.class);
    this.originalContext = Objects.requireNonNull(originalContext);
    this.asyncProcessorNode = originalContext.currentNode();
    this.delayedStoreWriter = Objects.requireNonNull(delayedStoreWriter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S extends StateStore> S getStateStore(final String name) {
    if (allStoreNameToAsyncStore.containsKey(name)) {
      return (S) allStoreNameToAsyncStore.get(name);
    }

    final S userDelegate = super.getStateStore(name);
    if (userDelegate instanceof TimestampedKeyValueStore) {
      final var asyncStore = new AsyncTimestampedKeyValueStore<>(
          name,
          taskId().partition(),
          (KeyValueStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNameToAsyncStore.put(name, asyncStore);
      kvStoreNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof KeyValueStore) {
      final var asyncStore = new AsyncKeyValueStore<>(
          name,
          originalContext.partition(),
          (KeyValueStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNameToAsyncStore.put(name, asyncStore);
      kvStoreNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof TimestampedWindowStore) {
      final var asyncStore = new AsyncTimestampedWindowStore<>(
          name,
          taskId().partition(),
          (WindowStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNameToAsyncStore.put(name, asyncStore);
      windowStoreNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof WindowStore) {
      final var asyncStore = new AsyncWindowStore<>(
          name,
          originalContext.partition(),
          (WindowStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNameToAsyncStore.put(name, asyncStore);
      windowStoreNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else {
      log.error("Attempted to connect session store with async processor");
      throw new UnsupportedOperationException(
          "Session stores are not yet supported with async processing");
    }
  }

  /**
   * (Re)set all inner state and metadata to prepare for a delayed async execution
   * such as processing input records or forwarding output records
   */
  public PreviousRecordContextAndNode prepareToFinalizeEvent(final AsyncEvent event) {
    if (!event.currentState().equals(State.TO_FINALIZE)) {
      log.error("Attempted to prepare event for finalization but currentState was {}",
                event.currentState());
      throw new IllegalStateException(
          "Must prepare event for finalization while it's in the TO_FINALIZE state"
      );
    }

    final ProcessorRecordContext recordContext = event.recordContext();

    // Note: the "RecordContext" and "RecordMetadata" refer to/are the same thing, and
    // even though they have separate getters with slightly different return types, they
    // both ultimately just return the recordContext we set here. So we don't need to
    // worry about setting the recordMetadata separately, even though #recordMetadata is
    // exposed to the user, since #setRecordContext takes care of that
    final PreviousRecordContextAndNode previousRecordContextAndNode
        = new PreviousRecordContextAndNode(
            originalContext.recordContext(),
            originalContext.currentNode(),
            originalContext
    );
    originalContext.setRecordContext(recordContext);
    originalContext.setCurrentNode(asyncProcessorNode);
    return previousRecordContextAndNode;
  }

  public <KS, VS> void executeDelayedWrite(
      final DelayedWrite<KS, VS> delayedWrite
  ) {
    final AsyncStateStore<KS, VS> asyncStore = getAsyncStore(delayedWrite.storeName());

    asyncStore.executeDelayedWrite(delayedWrite);
  }

  public <K extends KOut, V extends VOut> void executeDelayedForward(
      final DelayedForward<K, V> delayedForward
  ) {
    if (delayedForward.isFixedKey()) {
      super.forward(delayedForward.fixedKeyRecord(), delayedForward.childName());
    } else {
      super.forward(delayedForward.record(), delayedForward.childName());
    }
  }

  @Override
  public InternalProcessorContext<KOut, VOut> delegate() {
    return originalContext;
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> AsyncStateStore<KS, VS> getAsyncStore(final String storeName) {
    return (AsyncKeyValueStore<KS, VS>) kvStoreNameToAsyncStore.get(storeName);
  }

  public Map<String, StateStore> getAllAsyncStores() {
    final Map<String, StateStore> allStores = new HashMap<>(kvStoreNameToAsyncStore);
    allStores.putAll(windowStoreNameToAsyncStore);
    return allStores;
  }

  public static class PreviousRecordContextAndNode implements AutoCloseable {
    private final ProcessorRecordContext context;
    private final ProcessorNode<?, ?, ?, ?> node;
    private final InternalProcessorContext<?, ?> previousContext;

    public PreviousRecordContextAndNode(
        final ProcessorRecordContext context,
        final ProcessorNode<?, ?, ?, ?> node,
        final InternalProcessorContext<?, ?> previousContext) {
      this.context = context;
      this.node = node;
      this.previousContext = previousContext;
    }

    @Override
    public void close() {
      previousContext.setRecordContext(context);
      previousContext.setCurrentNode(node);
    }
  }
}
