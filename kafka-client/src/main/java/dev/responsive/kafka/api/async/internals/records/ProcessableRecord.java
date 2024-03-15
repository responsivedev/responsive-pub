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

import dev.responsive.kafka.internal.utils.ImmutableProcessorRecordContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A record that is to be processed asynchronously, and the metadata needed to do so
 * <p>
 * Threading notes:
 * -created by the StreamThread
 */
public class ProcessableRecord<KIn, VIn> implements AsyncRecord<KIn, VIn> {

  // Actual inputs to #forward
  private final Record<KIn, VIn> record;

  // Metadata for resetting the processor context before the #process
  private final ImmutableProcessorRecordContext recordContext;



  private final Runnable processListener;

  public ProcessableRecord(
      final Record<KIn, VIn> record,
      final ImmutableProcessorRecordContext recordContext,
      final Runnable process,
      final Runnable processListener
  ) {
    this.record = record;
    this.recordContext = recordContext;
    this.process = process;
    this.processListener = processListener;
  }

  public Record<KIn, VIn> record() {
    return record;
  }

  public ImmutableProcessorRecordContext recordContext() {
    return recordContext;
  }

  public Runnable process() {
    return process;
  }

  public Runnable processListener() {
    return processListener;
  }

  @Override
  public KIn key() {
    return record.key();
  }

  @Override
  public VIn value() {
    return record.value();
  }

  @Override
  public String topic() {
    return recordContext.topic();
  }

  @Override
  public long offset() {
    return recordContext.offset();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ProcessableRecord<?, ?> that = (ProcessableRecord<?, ?>) o;

    if (!record.equals(that.record)) {
      return false;
    }
    if (!recordContext.equals(that.recordContext)) {
      return false;
    }
    if (!process.equals(that.process)) {
      return false;
    }
    return processListener.equals(that.processListener);
  }

  @Override
  public int hashCode() {
    int result = record.hashCode();
    result = 31 * result + recordContext.hashCode();
    result = 31 * result + process.hashCode();
    result = 31 * result + processListener.hashCode();
    return result;
  }
}
