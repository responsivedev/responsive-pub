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

import dev.responsive.kafka.api.async.internals.records.ForwardableRecord;
import dev.responsive.kafka.api.async.internals.records.WriteableRecord;
import java.util.Map;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

/**
 * A utility class for handling delayed operations on async records, specifically forwarding
 * output records to the context and executing puts on the state store (which may trigger
 * cache evictions resulting in downstream processing similar to a forward).
 * <p>
 * Threading notes:
 * -To be executed on the StreamThread only
 * -One per physical AsyncProcessor instance
 */
public class DelayedRecordHandler<KOut, VOut> {

  private final ProcessorContextImpl originalContext;
  private final ProcessorNode<?, ?, ?, ?> currentNode;
  private final Map<String, AsyncKeyValueStore<?, ?>> asyncKVStores;

  public DelayedRecordHandler(
      final ProcessorContext<KOut, VOut> originalContext,
      final Map<String, AsyncKeyValueStore<?, ?>> asyncKVStores
  ) {
    this.originalContext = (ProcessorContextImpl) originalContext;
    this.currentNode = this.originalContext.currentNode();
    this.asyncKVStores = asyncKVStores;
  }

  @SuppressWarnings("unchecked")
  public <KS, VS> void executePut(final WriteableRecord<KS, VS> record) {
    final AsyncKeyValueStore<KS, VS> stateStore =
        (AsyncKeyValueStore<KS, VS>) asyncKVStores.get(record.storeName());

    stateStore.put(record.key(), record.value());
  }

  public void executeForward(final ForwardableRecord<KOut, VOut> record) {

    forwardToContext(
        record.record(),
        record.childName(),
        record.recordContext()
    );

    record.forwardListener().run();
  }

  private <K, V> void forwardToContext(
      final Record<K, V> record,
      final String childName,
      final AsyncProcessorRecordContext recordContext
  ) {
    originalContext.setCurrentNode(currentNode);

    // Note: the ProcessorContextImpl will actually update the recordContext during forward,
    // but only the timestamp and headers -- so we need to reset it to make sure the offset is
    // correct
    originalContext.setRecordContext(recordContext);

    originalContext.forward(record, childName);
  }
}
