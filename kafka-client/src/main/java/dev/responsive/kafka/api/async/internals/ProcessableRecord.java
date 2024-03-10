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

import java.util.function.Consumer;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * A record that is to be processed asynchronously, and the metadata needed to do so
 * <p>
 * Threading notes:
 * -created by the StreamThread
 */
public class ProcessableRecord<KIn, VIn> {

  // Actual inputs to #forward
  private final Record<KIn, VIn> record;

  // Metadata for resetting the processor context before the #process
  private final ProcessorRecordContext recordContext;

  // Executes the user's #process method on this record
  private final Consumer<Record<KIn, VIn>> process;

  public ProcessableRecord(
      final Record<KIn, VIn> record,
      final ProcessorRecordContext recordContext,
      final Consumer<Record<KIn, VIn>> process
  ) {
    this.record = record;
    this.recordContext = recordContext;
    this.process = process;
  }

  public Record<KIn, VIn> record() {
    return record;
  }

  public ProcessorRecordContext recordContext() {
    return recordContext;
  }

  public void process(final AsyncProcessorContext<?, ?> asyncContext) {
    // set context metadata needed for processing
    asyncContext.prepareForProcess(recordContext);
    process.accept(record);
  }

}
