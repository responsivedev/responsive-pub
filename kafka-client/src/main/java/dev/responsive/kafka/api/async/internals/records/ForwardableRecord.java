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

package dev.responsive.kafka.api.async.internals.records;

import dev.responsive.kafka.api.async.internals.records.ProcessableRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * A record that is ready and able to be forwarded by the StreamThread, plus the
 * metadata needed to do so. Essentially the output record of an async processor.
 * For the input record of an async processor, see {@link ProcessableRecord}
 * <p>
 * Threading notes:
 * -created by the AsyncThread but "executed" (forwarded) by the StreamThread
 */
public class ForwardableRecord<KOut, VOut> {

  // Actual inputs to #forward
  private final Record<KOut, VOut> record;
  private final String childName; // may be null

  // Metadata for resetting the processor context before the #forward
  private final ProcessorRecordContext recordContext;

  // Callback to notify listeners in the async processing architecture, for example
  // to unblock records waiting to be scheduled
  private final Runnable listener;

  public ForwardableRecord(
      final Record<KOut, VOut> record,
      final String childName,
      final ProcessorRecordContext recordContext,
      final Runnable listener
  ) {
    this.record = record;
    this.listener = listener;
    this.recordContext = recordContext;
    this.childName = childName;
  }

  public Record<KOut, VOut> record() {
    return record;
  }

  public String childName() {
    return childName;
  }

  public ProcessorRecordContext recordContext() {
    return recordContext;
  }

  public void notifyListeners() {
      listener.run();
  }
}
