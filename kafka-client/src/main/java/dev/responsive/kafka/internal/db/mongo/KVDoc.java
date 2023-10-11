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

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

public class KVDoc {

  @BsonId
  ObjectId id;
  byte[] key;
  byte[] value;
  long epoch;

  public KVDoc() {
  }

  public KVDoc(final byte[] key, final byte[] value, final long epoch) {
    this.key = key;
    this.value = value;
    this.epoch = epoch;
  }

  public ObjectId getId() {
    return id;
  }

  public void setId(final ObjectId id) {
    this.id = id;
  }

  public void setKey(final byte[] key) {
    this.key = key;
  }

  public void setValue(final byte[] value) {
    this.value = value;
  }

  public void setEpoch(final long epoch) {
    this.epoch = epoch;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public long getEpoch() {
    return epoch;
  }
}
