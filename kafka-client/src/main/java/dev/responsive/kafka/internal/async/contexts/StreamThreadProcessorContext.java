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
import dev.responsive.kafka.internal.async.events.AsyncEvent.State;
import dev.responsive.kafka.internal.async.events.DelayedForward;
import dev.responsive.kafka.internal.async.events.DelayedWrite;
import dev.responsive.kafka.internal.async.stores.AsyncKeyValueStore;
import dev.responsive.kafka.internal.async.stores.AsyncSessionStore;
import dev.responsive.kafka.internal.async.stores.AsyncStateStore;
import dev.responsive.kafka.internal.async.stores.AsyncTimestampedKeyValueStore;
import dev.responsive.kafka.internal.async.stores.AsyncTimestampedWindowStore;
import dev.responsive.kafka.internal.async.stores.AsyncWindowStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
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

  private final Map<String, AsyncStateStore<?, ?>> allStoreNamesToAsyncStore = new HashMap<>();

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
    if (allStoreNamesToAsyncStore.containsKey(name)) {
      return (S) allStoreNamesToAsyncStore.get(name);
    }

    final S userDelegate = super.getStateStore(name);
    if (userDelegate instanceof TimestampedKeyValueStore) {
      final var asyncStore = new AsyncTimestampedKeyValueStore<>(
          name,
          taskId().partition(),
          (KeyValueStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNamesToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof KeyValueStore) {
      final var asyncStore = new AsyncKeyValueStore<>(
          name,
          originalContext.partition(),
          (KeyValueStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNamesToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof TimestampedWindowStore) {
      final var asyncStore = new AsyncTimestampedWindowStore<>(
          name,
          taskId().partition(),
          (WindowStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNamesToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof WindowStore) {
      final var asyncStore = new AsyncWindowStore<>(
          name,
          originalContext.partition(),
          (WindowStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNamesToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else if (userDelegate instanceof SessionStore) {
      final var asyncStore = new AsyncSessionStore<>(
          name,
          originalContext.partition(),
          (SessionStore<?, ?>) userDelegate,
          delayedStoreWriter
      );
      allStoreNamesToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else {
      log.error("Attempted to add store {} which is not a type currently supported"
                    + " by async processing: {}",
                userDelegate.name(), userDelegate.getClass().getName());
      throw new UnsupportedOperationException(
          "Added store of unsupported type: " + userDelegate.name());
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

  @SuppressWarnings("unchecked")
  public <KS, VS> void executeDelayedWrite(
      final DelayedWrite<KS, VS> delayedWrite
  ) {
    final AsyncStateStore<KS, VS> asyncStore = (AsyncStateStore<KS, VS>) allStoreNamesToAsyncStore.get(delayedWrite.storeName());

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

  public Map<String, AsyncStateStore<?, ?>> getAllAsyncStores() {
    return allStoreNamesToAsyncStore;
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
