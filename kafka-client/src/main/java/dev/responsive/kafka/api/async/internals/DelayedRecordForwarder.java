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

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * A special kind of processor context that can only be used to forward records, but
 * allows forwarding to occur at an arbitrary time.
 * <p>
 * Threading notes:
 * -To be executed on the StreamThread only
 * -One per physical AsyncProcessor instance
 */
public class DelayedRecordForwarder<KOut, VOut> {

  private final ProcessorContextImpl actualContext;
  private final ProcessorNode<?, ?, ?, ?> currentNode;

  public DelayedRecordForwarder(final ProcessorContext<KOut, VOut> actualContext) {
    this.actualContext = (ProcessorContextImpl) actualContext;
    this.currentNode = this.actualContext.currentNode();
  }

  public void forward(final ForwardableRecord<KOut, VOut> forwardableRecord) {
    forwardableRecord.notifyListeners();

    forwardToContext(
        forwardableRecord.record(),
        forwardableRecord.childName(),
        forwardableRecord.recordContext()
    );
  }

  private <K, V> void forwardToContext(
      final Record<K, V> record,
      final String childName,
      final ProcessorRecordContext recordContext
  ) {
    actualContext.setCurrentNode(currentNode);

    // Note: the ProcessorContextImpl will actually update the recordContext during forward,
    // but only the timestamp and headers -- so we need to reset it to make sure the offset is
    // correct
    actualContext.setRecordContext(recordContext);

    actualContext.forward(record, childName);
  }
}
