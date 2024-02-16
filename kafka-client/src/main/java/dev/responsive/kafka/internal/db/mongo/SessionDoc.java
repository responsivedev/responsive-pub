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

import com.mongodb.BasicDBObject;
import dev.responsive.kafka.internal.utils.SessionKey;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;

public class SessionDoc {

  public static final String ID = "_id";
  public static final String VALUE = "value";
  public static final String EPOCH = "epoch";

  // Subfields of the composite key _id
  public static final String ID_RECORD_KEY = "key";
  public static final String ID_SESSION_START_TS = "sessionStartTs";
  public static final String ID_SESSION_END_TS = "sessionEndTs";

  BasicDBObject id;
  byte[] value;
  long epoch;

  public SessionDoc() {
  }

  public BasicDBObject getKey() {
    return id;
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

  public byte[] getValue() {
    return value;
  }

  public long getEpoch() {
    return epoch;
  }

  public static SessionKey sessionKey(final BasicDBObject compositeKey) {
    final byte[] key = (byte[]) compositeKey.get(ID_RECORD_KEY);
    final long sessionStartTimestamp = (long) compositeKey.get(ID_SESSION_START_TS);
    final long sessionEndTimestamp = (long) compositeKey.get(ID_SESSION_END_TS);
    return new SessionKey(key, sessionStartTimestamp, sessionEndTimestamp);
  }

  public static BasicDBObject compositeKey(
      final byte[] key,
      final long sessionStartTs,
      final long sessionEndTs
  ) {
    final BasicDBObject compositeKey =
        new BasicDBObject(ID_RECORD_KEY, key).
            append(ID_SESSION_START_TS, sessionStartTs).
            append(ID_SESSION_END_TS, sessionEndTs);
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
    final SessionDoc windowDoc = (SessionDoc) o;
    return epoch == windowDoc.epoch
        && Objects.equals(id, windowDoc.id)
        && Arrays.equals(value, windowDoc.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, epoch);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    final SessionKey sessionKey = sessionKey(id);
    return "SessionDoc{"
        + "id=" + Arrays.toString(sessionKey.key.get())
        + ", sessionStartTs=" + sessionKey.sessionStartMs
        + ", sessionEndTs=" + sessionKey.sessionEndMs
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + '}';
  }
}
