/*
 * Copyright 2024 Responsive Computing, Inc.
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

import com.mongodb.BasicDBObject;
import dev.responsive.kafka.internal.utils.SessionKey;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class SessionDoc {

  public static final String ID = "_id";
  public static final String VALUE = "value";
  public static final String EPOCH = "epoch";
  public static final String TOMBSTONE_TS = "tombstoneTs";

  // Subfields of the composite key _id
  public static final String ID_RECORD_KEY = "key";
  public static final String ID_SESSION_END_MS = "sessionEndMs";
  public static final String ID_SESSION_START_MS = "sessionStartMs";

  BasicDBObject id;
  byte[] value;
  long epoch;
  Date tombstoneTs;

  public SessionDoc() {
  }

  @BsonCreator
  public SessionDoc(
      @BsonProperty(ID) BasicDBObject id,
      @BsonProperty(VALUE) byte[] value,
      @BsonProperty(EPOCH) long epoch,
      @BsonProperty(TOMBSTONE_TS) Date tombstoneTs
  ) {
    this.id = id;
    this.value = value;
    this.epoch = epoch;
    this.tombstoneTs = tombstoneTs;
  }

  public BasicDBObject key() {
    return id;
  }

  public byte[] value() {
    return value;
  }

  public long epoch() {
    return epoch;
  }

  public void setKey(final BasicDBObject id) {
    this.id = id;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public SessionKey toSessionKey() {
    final byte[] key = (byte[]) id.get(ID_RECORD_KEY);
    final long sessionEndTimestamp = (long) id.get(ID_SESSION_END_MS);
    final long sessionStartTimestamp = (long) id.get(ID_SESSION_START_MS);
    return new SessionKey(key, sessionStartTimestamp, sessionEndTimestamp);
  }

  public static BasicDBObject compositeKey(
      final byte[] key,
      final long sessionStartTs,
      final long sessionEndTs
  ) {
    final BasicDBObject compositeKey =
        new BasicDBObject(ID_RECORD_KEY, key)
            .append(ID_SESSION_END_MS, sessionEndTs)
            .append(ID_SESSION_START_MS, sessionStartTs);
    return compositeKey;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionDoc sessionDoc = (SessionDoc) o;
    return epoch == sessionDoc.epoch
        && Objects.equals(id, sessionDoc.id)
        && Arrays.equals(value, sessionDoc.value)
        && Objects.equals(tombstoneTs, sessionDoc.tombstoneTs);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, epoch, tombstoneTs);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    final SessionKey sessionKey = this.toSessionKey();
    return "SessionDoc{"
        + "id=" + Arrays.toString(sessionKey.key.get())
        + ", sessionStartTs=" + sessionKey.sessionStartMs
        + ", sessionEndTs=" + sessionKey.sessionEndMs
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + ", tombstoneTs=" + tombstoneTs
        + '}';
  }
}
