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
  public static final String ID = "_id";
  public static final String VALUE = "value";
  public static final String EPOCH = "epoch";
  public static final String TOMBSTONE_TS = "tombstoneTs";

  @BsonId
  byte[] id;
  byte[] value;
  long epoch;
  Date tombstoneTs;

  public KVDoc() {
  }

  public KVDoc(final byte[] key, final byte[] value, final long epoch) {
    this.value = value;
    this.epoch = epoch;
  }

  public byte[] getKey() {
    return id;
  }

  public void setKey(final byte[] id) {
    this.id = id;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public byte[] getValue() {
    return value;
  }

  public long getEpoch() {
    return epoch;
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
        && Arrays.equals(id, kvDoc.id)
        && Arrays.equals(value, kvDoc.value)
        && Objects.equals(tombstoneTs, kvDoc.tombstoneTs);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, epoch, tombstoneTs);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "KVDoc{"
        + "id=" + Arrays.toString(id)
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + ", tombstoneTs=" + tombstoneTs
        + '}';
  }
}
