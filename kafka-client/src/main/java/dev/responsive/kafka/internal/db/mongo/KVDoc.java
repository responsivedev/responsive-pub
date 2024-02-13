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

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import org.bson.codecs.pojo.annotations.BsonId;

public class KVDoc {

  // TODO(agavra): figure out if we can use @BsonProperty to set the names explicitly
  public static final String KEY = "_id";
  public static final String VALUE = "value";
  public static final String KAFKA_PARTITION = "partition";
  public static final String EPOCH = "epoch";
  public static final String TOMBSTONE_TS = "tombstoneTs";

  @BsonId
  byte[] key;
  byte[] value;
  int kafkaPartition;
  long epoch;
  Date tombstoneTs;

  public KVDoc() {
  }

  public KVDoc(final byte[] key, final byte[] value, final int kafkaPartition, final long epoch) {
    this.key = key;
    this.value = value;
    this.kafkaPartition = kafkaPartition;
    this.epoch = epoch;
  }

  public byte[] getKey() {
    return key;
  }

  public void setKey(final byte[] id) {
    this.key = id;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public int getKafkaPartition() {
    return kafkaPartition;
  }

  public void setKafkaPartition(final int kafkaPartition) {
    this.kafkaPartition = kafkaPartition;
  }

  public Date getTombstoneTs() {
    return tombstoneTs;
  }

  public void setTombstoneTs(final Date tombstoneTs) {
    this.tombstoneTs = tombstoneTs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KVDoc other = (KVDoc) o;
    return epoch == other.epoch
        && kafkaPartition == other.kafkaPartition
        && Arrays.equals(key, other.key)
        && Arrays.equals(value, other.value)
        && Objects.equals(tombstoneTs, other.tombstoneTs);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(kafkaPartition, epoch, tombstoneTs);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "KVDoc{"
        + "id=" + Arrays.toString(key)
        + ", value=" + Arrays.toString(value)
        + ", kafkaPartition=" + kafkaPartition
        + ", epoch=" + epoch
        + ", tombstoneTs=" + tombstoneTs
        + '}';
  }
}
