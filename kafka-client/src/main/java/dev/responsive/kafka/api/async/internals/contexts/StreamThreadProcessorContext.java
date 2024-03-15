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
import dev.responsive.kafka.api.async.internals.AsyncThread.CurrentAsyncEvent;
import dev.responsive.kafka.api.async.internals.queues.WritingQueue;
import dev.responsive.kafka.api.async.internals.records.ForwardableRecord;
import dev.responsive.kafka.api.async.internals.records.WriteableRecord;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
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

  private final WritingQueue<?, ?> writingQueue;
  private final Map<String, AsyncKeyValueStore<?, ?>> storeNameToAsyncStore = new HashMap<>();
  private final Map<String, CurrentAsyncEvent> asyncThreadToCurrentEvent;

  public StreamThreadProcessorContext(
      final ProcessorContext<?, ?> delegate,
      final WritingQueue<?, ?> writingQueue,
      final Map<String, CurrentAsyncEvent> asyncThreadToCurrentEvent
  ) {
    super(delegate);
    this.writingQueue = writingQueue;
    this.asyncThreadToCurrentEvent = asyncThreadToCurrentEvent;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S extends StateStore> S getStateStore(final String name) {
    final S userDelegate = super.getStateStore(name);
    if (userDelegate instanceof KeyValueStore) {
      final var asyncStore = new AsyncKeyValueStore<>(
          name,
          (KeyValueStore<?, ?>) userDelegate,
          writingQueue,
          asyncThreadToCurrentEvent
      );
      storeNameToAsyncStore.put(name, asyncStore);
      return (S) asyncStore;
    } else {
      throw new UnsupportedOperationException(
          "Window and Session stores are not yet supported with async processing");
    }
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> void delayedWrite(
      final WriteableRecord<KS, VS> writeableRecord
  ) {
    super.prepareForAsyncExecution(writeableRecord.recordContext());

    final AsyncKeyValueStore<KS, VS> asyncStore =
        (AsyncKeyValueStore<KS, VS>) storeNameToAsyncStore.get(writeableRecord.storeName());
    asyncStore.executeDelayedPut(writeableRecord);

    writeableRecord.markWriteAsComplete();
  }

  public <K extends KOut, V extends VOut> void delayedForward(
      final ForwardableRecord<K, V> forwardableRecord
  ) {
    super.prepareForAsyncExecution(forwardableRecord.recordContext());

    // super.forward throws an exception, so we have to issue the forward to the delegate directly
    super.delegate().forward(forwardableRecord.record(), forwardableRecord.childName());

    forwardableRecord.markForwardAsComplete();
  }

  public AsyncKeyValueStore<?, ?> getAsyncStore(final String storeName) {
    return storeNameToAsyncStore.get(storeName);
  }

  public Map<String, AsyncKeyValueStore<?, ?>> getAllAsyncStores() {
    return storeNameToAsyncStore;
  }

}
