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

public class MetadataDoc {

  @BsonId
  ObjectId id;
  int partition;
  long offset;

  public MetadataDoc() {
  }

  public ObjectId id() {
    return id;
  }

  public void setId(final ObjectId id) {
    this.id = id;
  }

  public int partition() {
    return partition;
  }

  public void setPartition(final int partition) {
    this.partition = partition;
  }

  public long offset() {
    return offset;
  }

  public void setOffset(final long offset) {
    this.offset = offset;
  }
}
