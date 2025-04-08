/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Objects;

public class WindowedPut extends Put {
  private final long timestamp;
  private final long windowTimestamp;

  public WindowedPut(
      final byte[] key,
      final byte[] value,
      final long timestamp,
      final long windowTimestamp
  ) {
    super(key, value);
    this.timestamp = timestamp;
    this.windowTimestamp = windowTimestamp;
  }

  public long timestamp() {
    return timestamp;
  }

  public long windowTimestamp() {
    return windowTimestamp;
  }

  @Override
  public void visit(final Visitor visitor) {
    visitor.visit(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final WindowedPut that = (WindowedPut) o;
    return timestamp == that.timestamp && windowTimestamp == that.windowTimestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timestamp, windowTimestamp);
  }

}
