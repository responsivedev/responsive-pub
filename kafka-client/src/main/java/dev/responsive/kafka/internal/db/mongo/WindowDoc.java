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

import com.mongodb.BasicDBObject;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class WindowDoc {

  public static final String ID = "_id";
  public static final String VALUE = "value";
  public static final String VALUES = "values";
  public static final String EPOCH = "epoch";

  // Subfields of the composite key _id
  public static final String ID_RECORD_KEY = "key";
  public static final String ID_WINDOW_START_TS = "windowStartTs";

  BasicDBObject id;
  byte[] value;
  List<byte[]> values;
  long epoch;

  public WindowDoc() {
  }

  @BsonCreator
  public WindowDoc(
      @BsonProperty(ID) BasicDBObject id,
      @BsonProperty(VALUE) byte[] value,
      @BsonProperty(VALUES) List<byte[]> values,
      @BsonProperty(EPOCH) long epoch
  ) {
    this.id = id;
    this.value = value;
    this.values = values;
    this.epoch = epoch;
  }

  public BasicDBObject getKey() {
    return id;
  }

  public void setKey(final BasicDBObject id) {
    this.id = id;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public List<byte[]> getValues() {
    return values;
  }

  public void setValues(final List<byte[]> values) {
    this.values = values;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public String unwrapRecordKey() {
    return (String) id.get(ID_RECORD_KEY);
  }

  public long unwrapWindowStartTs() {
    return (long) id.get(ID_WINDOW_START_TS);
  }

  public static BasicDBObject compositeKey(
      final String key,
      final long windowStartTs,
      final boolean timestampFirstOrder
  ) {
    if (timestampFirstOrder) {
      final BasicDBObject compositeKey = new BasicDBObject(ID_WINDOW_START_TS, windowStartTs);
      return compositeKey.append(ID_RECORD_KEY, key);
    } else {
      final BasicDBObject compositeKey = new BasicDBObject(ID_RECORD_KEY, key);
      return compositeKey.append(ID_WINDOW_START_TS, windowStartTs);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowDoc windowDoc = (WindowDoc) o;
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
    return "WindowDoc{"
        + "id=" + unwrapRecordKey()
        + ", windowStartTs=" + unwrapWindowStartTs()
        + ", value=" + Arrays.toString(value)
        + ", epoch=" + epoch
        + '}';
  }
}
