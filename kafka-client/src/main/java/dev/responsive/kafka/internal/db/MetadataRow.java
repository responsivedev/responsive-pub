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

package dev.responsive.kafka.internal.db;

import java.util.Objects;

public class MetadataRow {

  public final long offset;
  public final long epoch;

  public MetadataRow(final long offset, final long epoch) {
    this.offset = offset;
    this.epoch = epoch;
  }

  @Override
  public String toString() {
    return "MetadataRow{"
        + "offset=" + offset
        + ", epoch=" + epoch
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MetadataRow that = (MetadataRow) o;
    return offset == that.offset && epoch == that.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, epoch);
  }
}
