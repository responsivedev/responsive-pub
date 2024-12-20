/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.api.async.internals.events;


/**
 * A record (or tombstone) that should be written to the given state store.
 * These records are created by the AsyncStateStore when a put is intercepted
 * by an AsyncThread. They are then added to the corresponding AsyncEvent, which
 * will handle the handoff from AsyncThread back to StreamThread, and ultimately
 * ensure all delayed writes are executed by that StreamThread during the
 * finalization phase of an async event.
 */
public class DelayedWrite<KS, VS> {

  private final String storeName;

  private final KS recordKey;
  private final VS recordValue;

  // only for window stores
  private final long windowStartMs;

  public static <KS, VS> DelayedWrite<KS, VS> newKVWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue
  ) {
    return new DelayedWrite<>(storeName, recordKey, recordValue, 0L);
  }

  public static <KS, VS> DelayedWrite<KS, VS> newWindowWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue,
      final long windowStartMs
  ) {
    return new DelayedWrite<>(storeName, recordKey, recordValue, windowStartMs);
  }

  private DelayedWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue,
      final long windowStartMs
  ) {
    this.recordKey = recordKey;
    this.recordValue = recordValue;
    this.storeName = storeName;
    this.windowStartMs = windowStartMs;
  }

  public String storeName() {
    return storeName;
  }

  public KS key() {
    return recordKey;
  }

  public VS value() {
    return recordValue;
  }

  public long windowStartMs() {
    return windowStartMs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DelayedWrite<?, ?> that = (DelayedWrite<?, ?>) o;

    if (!recordKey.equals(that.recordKey)) {
      return false;
    }
    return storeName.equals(that.storeName);
  }

  @Override
  public int hashCode() {
    return 31 * recordKey.hashCode() + storeName.hashCode();
  }

  @Override
  public String toString() {
    return "DelayedWrite<" + recordKey + ", " + recordValue + ">";
  }
}
