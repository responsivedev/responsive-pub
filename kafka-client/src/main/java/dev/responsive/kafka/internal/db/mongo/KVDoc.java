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
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class KVDoc {

  // TODO(agavra): figure out if we can use @BsonProperty to set the names explicitly
  public static final String ID = "_id";
  public static final String VALUE = "value";
  public static final String EPOCH = "epoch";
  public static final String TIMESTAMP = "ts";
  public static final String KAFKA_PARTITION = "partition";
  public static final String TOMBSTONE_TS = "tombstoneTs";

  // We use a string key for ID because mongo range scans don't work as expected for binary
  // fields. In particular, mongo orders binary fields by length first, and then by the contained
  // data. Therefore, we encode the id as a string so that we get the desired range behaviour.
  String id;
  byte[] value;
  long epoch;
  long timestamp;
  int kafkaPartition;
  Date tombstoneTs;

  public KVDoc() {
  }

  @BsonCreator
  public KVDoc(
      @BsonProperty(ID) String id,
      @BsonProperty(VALUE) byte[] value,
      @BsonProperty(EPOCH) long epoch,
      @BsonProperty(TIMESTAMP) long timestamp,
      @BsonProperty(KAFKA_PARTITION) int kafkaPartition
  ) {
    this.id = id;
    this.value = value;
    this.epoch = epoch;
    this.timestamp = timestamp;
    this.kafkaPartition = kafkaPartition;
  }

  public String getKey() {
    return id;
  }

  public void setKey(final String id) {
    this.id = id;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  public byte[] getValue() {
    return value;
  }

  public long getEpoch() {
    return epoch;
  }

  public long getTimestamp() {
    return timestamp;
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
    final KVDoc kvDoc = (KVDoc) o;
    return epoch == kvDoc.epoch
        && Objects.equals(id, kvDoc.id)
        && Arrays.equals(value, kvDoc.value)
        && Objects.equals(timestamp, kvDoc.timestamp)
        && Objects.equals(kafkaPartition, kvDoc.kafkaPartition)
        && Objects.equals(tombstoneTs, kvDoc.tombstoneTs);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, epoch, tombstoneTs, timestamp, kafkaPartition);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "KVDoc{"
        + "id=" + id
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + ", tombstoneTs=" + tombstoneTs
        + ", timestamp=" + timestamp
        + ", kafkaPartition=" + kafkaPartition
        + '}';
  }
}