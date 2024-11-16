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

package dev.responsive.kafka.internal.db.mongo;

import java.util.Objects;

/**
 * The 'id' of a window metadata doc corresponds to the kafka partition,
 * which is why we don't have to include the partition as a separate
 * field for the metadata docs the way we do for the data docs
 */
public class WindowMetadataDoc {

  public static final String PARTITION = "_id";
  public static final String OFFSET = "offset";
  public static final String EPOCH = "epoch";
  public static final String STREAM_TIME = "streamTime";

  int partition;
  long offset;
  long epoch;
  long streamTime;

  public WindowMetadataDoc() {
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

  public long streamTime() {
    return streamTime;
  }

  public void setStreamTime(final long streamTime) {
    this.streamTime = streamTime;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowMetadataDoc that = (WindowMetadataDoc) o;
    return partition == that.partition
        && offset == that.offset
        && epoch == that.epoch
        && streamTime == that.streamTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, epoch, offset, streamTime);
  }

  @Override
  public String toString() {
    return "WindowMetadataDoc{"
        + "id=" + partition
        + ", offset=" + offset
        + ", epoch=" + epoch
        + ", streamTime=" + streamTime
        + '}';

  }
}

