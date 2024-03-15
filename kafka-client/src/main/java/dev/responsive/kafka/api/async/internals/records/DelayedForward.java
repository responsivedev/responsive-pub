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

import java.util.Objects;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A container class representing a "delayed forward" ie a single invocation of
 * {@link ProcessorContext#forward} by the user inside their #process implementation,
 * that was intercepted by the async processing framework and not immediately executed.
 * <p>
 * This class holds all the input parameters that were passed in to the corresponding
 * #forward call, and is itself contained and tracked by the AsyncEvent being processed
 * when this forward was initiated. The AsyncEvent is responsible for ensuring that all
 * delayed forwards it receives are eventually executed with the correct inputs and
 * any required metadata. A delayed forward will remain buffered until the AsyncEvent
 * completes its processing phase and is returned to the original StreamThread for
 * "finalization". During finalization, all outputs from the processing phase (which
 * includes delayed forwards and also delayed writes) will be executed in order until
 * the buffers are empty, at which time the AsyncEvent is officially "done".
 * <p>
 * Threading notes:
 * -created by the AsyncThread but ultimately "executed" (forwarded) by the StreamThread
 */
public class DelayedForward<KOut, VOut> {

  // Actual inputs to #forward
  private final Record<KOut, VOut> record;
  private final String childName; // may be null

  public DelayedForward(
      final Record<KOut, VOut> record,
      final String childName
  ) {
    this.record = record;
    this.childName = childName;
  }

  public Record<KOut, VOut> record() {
    return record;
  }

  public String childName() {
    return childName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DelayedForward<?, ?> that = (DelayedForward<?, ?>) o;

    if (!record.equals(that.record)) {
      return false;
    }
    return Objects.equals(childName, that.childName);
  }

  @Override
  public int hashCode() {
    return 31 * record.hashCode() + (childName != null ? childName.hashCode() : 0);
  }
}
