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

package dev.responsive.kafka.internal.async.events;

import java.util.Objects;
import org.apache.kafka.streams.kstream.Windowed;

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

  // window stores only
  private final long windowStartMs;

  // session stores only
  private final Windowed<KS> sessionKey;

  public static <KS, VS> DelayedWrite<KS, VS> newKVWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue
  ) {
    return new DelayedWrite<>(storeName, recordKey, recordValue, 0L, null);
  }

  public static <KS, VS> DelayedWrite<KS, VS> newWindowWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue,
      final long windowStartMs
  ) {
    return new DelayedWrite<>(storeName, recordKey, recordValue, windowStartMs, null);
  }

  public static <KS, VS> DelayedWrite<KS, VS> newSessionWrite(
      final String storeName,
      final Windowed<KS> sessionKey,
      final VS recordValue
  ) {
    return new DelayedWrite<>(storeName, null, recordValue, 0L, sessionKey);
  }

  private DelayedWrite(
      final String storeName,
      final KS recordKey,
      final VS recordValue,
      final long windowStartMs,
      final Windowed<KS> sessionKey
  ) {
    this.recordKey = recordKey;
    this.recordValue = recordValue;
    this.storeName = storeName;
    this.windowStartMs = windowStartMs;
    this.sessionKey = sessionKey;
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

  public Windowed<KS> sessionKey() {
    return sessionKey;
  }

  @Override
  public String toString() {
    return "DelayedWrite{" +
        "storeName='" + storeName + '\'' +
        ", recordKey=" + recordKey +
        ", recordValue=" + recordValue +
        ", windowStartMs=" + windowStartMs +
        ", sessionKey=" + sessionKey +
        '}';
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

    if (windowStartMs != that.windowStartMs) {
      return false;
    }
    if (!storeName.equals(that.storeName)) {
      return false;
    }
    if (!Objects.equals(recordKey, that.recordKey)) {
      return false;
    }
    if (!Objects.equals(recordValue, that.recordValue)) {
      return false;
    }
    return Objects.equals(sessionKey, that.sessionKey);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + (recordKey != null ? recordKey.hashCode() : 0);
    result = 31 * result + (recordValue != null ? recordValue.hashCode() : 0);
    result = 31 * result + (int) (windowStartMs ^ (windowStartMs >>> 32));
    result = 31 * result + (sessionKey != null ? sessionKey.hashCode() : 0);
    return result;
  }
}
