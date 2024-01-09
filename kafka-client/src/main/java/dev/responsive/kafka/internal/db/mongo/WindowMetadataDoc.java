/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db.mongo;

import java.util.Objects;

/**
 * The 'id' of a window metadata doc corresponds to the kafka partition,
 * which is why we don't have to include the partition as a separate
 * field for the metadata docs the way we do for the data docs
 */
public class WindowMetadataDoc {

  public static final String ID = "_id";
  public static final String OFFSET = "offset";
  public static final String EPOCH = "epoch";
  public static final String STREAM_TIME = "streamTime";

  int id;
  long offset;
  long epoch;
  long streamTime;

  public WindowMetadataDoc() {
  }

  public int id() {
    return id;
  }

  public void setId(final int id) {
    this.id = id;
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
    return id == that.id
        && offset == that.offset
        && epoch == that.epoch
        && streamTime == that.streamTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, epoch, offset, streamTime);
  }

  @Override
  public String toString() {
    return "WindowMetadataDoc{"
        + "id=" + id
        + ", offset=" + offset
        + ", epoch=" + epoch
        + ", streamTime=" + streamTime
        + '}';

  }
}

