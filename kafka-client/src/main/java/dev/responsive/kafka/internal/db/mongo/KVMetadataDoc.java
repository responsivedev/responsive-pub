/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.db.mongo;

import java.util.Objects;

public class KVMetadataDoc {

  public static final String KAFKA_PARTITION = "_id";
  public static final String OFFSET = "offset";
  public static final String EPOCH = "epoch";

  int kafkaPartition;
  long offset;
  long epoch;

  public KVMetadataDoc() {
  }

  public int partition() {
    return kafkaPartition;
  }

  public void setKafkaPartition(final int kafkaPartition) {
    this.kafkaPartition = kafkaPartition;
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
    return kafkaPartition == that.kafkaPartition
        && offset == that.offset
        && epoch == that.epoch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaPartition, epoch, offset);
  }

  @Override
  public String toString() {
    return "KVMetadataDoc{"
        + ", partition=" + kafkaPartition
        + ", offset=" + offset
        + ", epoch=" + epoch
        + '}';

  }
}
