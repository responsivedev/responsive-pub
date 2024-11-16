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

import com.mongodb.BasicDBObject;
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

  public String unwrapRecordKey() {
    return (String) id.get(ID_RECORD_KEY);
  }

  public long unwrapSessionEndMs() {
    return (long) id.get(ID_SESSION_END_MS);
  }

  public long unwrapSessionStartMs() {
    return (long) id.get(ID_SESSION_START_MS);
  }

  public static BasicDBObject compositeKey(
      final String key,
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
    return "SessionDoc{"
        + "id=" + unwrapRecordKey()
        + ", sessionStartTs=" + unwrapSessionStartMs()
        + ", sessionEndTs=" + unwrapSessionEndMs()
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + ", tombstoneTs=" + tombstoneTs
        + '}';
  }
}
