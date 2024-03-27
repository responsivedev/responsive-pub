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

import dev.responsive.kafka.api.async.internals.events.DelayedForward;
import dev.responsive.kafka.api.async.internals.events.DelayedWrite;
import dev.responsive.kafka.api.async.internals.stores.AsyncKeyValueStore;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;

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
    extends DelegatingInternalProcessorContext<KOut, VOut> {

  private final Map<String, AsyncKeyValueStore<?, ?>> storeNameToAsyncStore = new HashMap<>();
  private final ProcessorNode<?, ?, ?, ?> asyncProcessorNode;

  public StreamThreadProcessorContext(
      final ProcessorContext<KOut, VOut> delegate
  ) {
    super((InternalProcessorContext<KOut, VOut>) delegate);

    asyncProcessorNode = super.currentNode();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S extends StateStore> S getStateStore(final String name) {
    final S userDelegate = super.getStateStore(name);
    if (userDelegate instanceof KeyValueStore) {
      final var asyncStore = new AsyncKeyValueStore<>(
          name,
          super.partition(),
          (KeyValueStore<?, ?>) userDelegate
      );
      storeNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else {
      throw new UnsupportedOperationException(
          "Window and Session stores are not yet supported with async processing");
    }
  }

  /**
   * (Re)set all inner state and metadata to prepare for a delayed async execution
   * such as processing input records or forwarding output records
   */
  public void prepareToFinalizeEvent(final ProcessorRecordContext recordContext) {
    // Note: the "RecordContext" and "RecordMetadata" refer to/are the same thing, and
    // even though they have separate getters with slightly different return types, they
    // both ultimately just return the recordContext we set here. So we don't need to
    // worry about setting the recordMetadata separately, even though #recordMetadata is
    // exposed to the user, since #setRecordContext takes care of that
    super.setRecordContext(recordContext);
    super.setCurrentNode(asyncProcessorNode);
  }

  public <KS, VS> void executeDelayedWrite(
      final DelayedWrite<KS, VS> delayedWrite
  ) {
    final AsyncKeyValueStore<KS, VS> asyncStore =
        getAsyncStore(delayedWrite.storeName());

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

  @SuppressWarnings("unchecked")
  public <KS, VS> AsyncKeyValueStore<KS, VS> getAsyncStore(final String storeName) {
    return (AsyncKeyValueStore<KS, VS>) storeNameToAsyncStore.get(storeName);
  }

  public Map<String, AsyncKeyValueStore<?, ?>> getAllAsyncStores() {
    return storeNameToAsyncStore;
  }

}
