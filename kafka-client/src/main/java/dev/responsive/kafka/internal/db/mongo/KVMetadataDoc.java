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

package dev.responsive.kafka.internal.db.mongo;

import java.util.Objects;

public class KVMetadataDoc {

  public static final String PARTITION = "_id";
  public static final String OFFSET = "offset";
  public static final String EPOCH = "epoch";

  int partition;
  long offset;
  long epoch;

  public KVMetadataDoc() {
  }

  public int partition() {
    return partition;
  }

  public void setPartition(final int partition) {
    this.partition = partition;
  }

  public long offset() {
    return offset;
  }

  public void setOffset(final long offset) {
    this.offset = offset;
  }

  public long epoch() {
    return epoch;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KVMetadataDoc that = (KVMetadataDoc) o;
    return partition == that.partition
        && offset == that.offset
        && epoch == that.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, epoch, offset);
  }

  @Override
  public String toString() {
    return "KVMetadataDoc{"
        + ", partition=" + partition
        + ", offset=" + offset
        + ", epoch=" + epoch
        + '}';

  }
}
