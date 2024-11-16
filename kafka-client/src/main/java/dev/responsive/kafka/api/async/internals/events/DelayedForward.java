/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.api.async.internals.events;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
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

  // Exactly one record is non-null, and the other is always null
  // Sadly these don't extend a common API in Streams, so this messy handling
  // we have to do is the same way Streams handles these
  private final FixedKeyRecord<KOut, VOut> fixedKeyRecord;
  private final Record<KOut, VOut> record;

  private final String childName; // may be null

  public static <KOut, VOut> DelayedForward<KOut, VOut> ofRecord(
      final Record<KOut, VOut> record,
      final String childName
  ) {
    return new DelayedForward<>(record, null, childName);
  }

  public static <KOut, VOut> DelayedForward<KOut, VOut> ofFixedKeyRecord(
      final FixedKeyRecord<KOut, VOut> fixedKeyRecord,
      final String childName
  ) {
    return new DelayedForward<>(null, fixedKeyRecord, childName);
  }

  private DelayedForward(
      final Record<KOut, VOut> record,
      final FixedKeyRecord<KOut, VOut> fixedKeyRecord,
      final String childName
  ) {
    this.record = record;
    this.fixedKeyRecord = fixedKeyRecord;
    this.childName = childName;

    if (record == null && fixedKeyRecord == null) {
      throw new IllegalStateException("Both record and fixedKeyRecord were null");
    } else if (record != null && fixedKeyRecord != null) {
      throw new IllegalStateException("Both record and fixedKeyRecord were non-null");
    }
  }

  public boolean isFixedKey() {
    return fixedKeyRecord != null;
  }

  public Record<KOut, VOut> record() {
    return record;
  }

  public FixedKeyRecord<KOut, VOut> fixedKeyRecord() {
    return fixedKeyRecord;
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

  @Override
  public String toString() {
    if (isFixedKey()) {
      return String.format("DelayedForward<%s, %s>", fixedKeyRecord.key(), fixedKeyRecord.value());
    } else {
      return String.format("DelayedForward<%s, %s>", record.key(), record.value());
    }
  }
}
