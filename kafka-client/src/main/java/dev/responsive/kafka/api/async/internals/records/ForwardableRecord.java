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

import dev.responsive.kafka.api.async.internals.AsyncProcessorRecordContext;
import java.util.Objects;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A record that is ready and able to be forwarded by the StreamThread, plus the
 * metadata needed to do so. Essentially the output record of an async processor.
 * For the input record of an async processor, see {@link ProcessableRecord}
 * <p>
 * Threading notes:
 * -created by the AsyncThread but "executed" (forwarded) by the StreamThread
 */
public class ForwardableRecord<KOut, VOut> implements AsyncRecord<KOut, VOut> {

  // Actual inputs to #forward
  private final Record<KOut, VOut> record;
  private final String childName; // may be null

  // Metadata for resetting the processor context before the #forward
  private final AsyncProcessorRecordContext recordContext;

  // Callback to notify listeners in the async processing architecture, for example
  // to unblock records waiting to be scheduled
  private final Runnable forwardListener;

  public ForwardableRecord(
      final Record<KOut, VOut> record,
      final String childName,
      final AsyncProcessorRecordContext recordContext,
      final Runnable forwardListener
  ) {
    this.record = record;
    this.forwardListener = forwardListener;
    this.recordContext = recordContext;
    this.childName = childName;
  }

  public Record<KOut, VOut> record() {
    return record;
  }

  public String childName() {
    return childName;
  }

  public AsyncProcessorRecordContext recordContext() {
    return recordContext;
  }

  public Runnable forwardListener() {
      return forwardListener;
  }

  @Override
  public KOut key() {
    return record.key();
  }

  @Override
  public VOut value() {
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

    final ForwardableRecord<?, ?> that = (ForwardableRecord<?, ?>) o;

    if (!record.equals(that.record)) {
      return false;
    }
    if (!Objects.equals(childName, that.childName)) {
      return false;
    }
    if (!recordContext.equals(that.recordContext)) {
      return false;
    }
    return forwardListener.equals(that.forwardListener);
  }

  @Override
  public int hashCode() {
    int result = record.hashCode();
    result = 31 * result + (childName != null ? childName.hashCode() : 0);
    result = 31 * result + recordContext.hashCode();
    result = 31 * result + forwardListener.hashCode();
    return result;
  }
}
