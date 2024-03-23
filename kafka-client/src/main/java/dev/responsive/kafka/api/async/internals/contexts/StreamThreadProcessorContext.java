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

import dev.responsive.kafka.api.async.internals.stores.AsyncKeyValueStore;
import dev.responsive.kafka.api.async.internals.events.DelayedForward;
import dev.responsive.kafka.api.async.internals.events.DelayedWrite;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A specific variant of the async processor context to be used by the StreamThread.
 * This context handles everything related to initialization of the processor
 * (eg {@link #getStateStore(String)}) and enables delayed forwarding of output records.
 * <p>
 * Threading notes:
 * -For use by StreamThreads only
 * -One per StreamThread per task
 */
public class StreamThreadProcessorContext<KOut, VOut> extends AsyncProcessorContext<KOut, VOut> {

  private final Map<String, AsyncKeyValueStore<?, ?>> storeNameToAsyncStore = new HashMap<>();

  public StreamThreadProcessorContext(
      final ProcessorContext<KOut, VOut> delegate
  ) {
    super(delegate);
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

  public void prepareToFinalizeEvent(final ProcessorRecordContext recordContext) {
    super.prepareForDelayedExecution(recordContext);
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
    // super.forward throws an exception to prevent forwarding by other context types,
    // so we have to issue the forward by going through the delegate directly
    delegate().forward(delayedForward.record(), delayedForward.childName());
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> AsyncKeyValueStore<KS, VS> getAsyncStore(final String storeName) {
    return (AsyncKeyValueStore<KS, VS>) storeNameToAsyncStore.get(storeName);
  }

  public Map<String, AsyncKeyValueStore<?, ?>> getAllAsyncStores() {
    return storeNameToAsyncStore;
  }

}
