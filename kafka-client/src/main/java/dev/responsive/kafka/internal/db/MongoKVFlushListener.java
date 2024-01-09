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

package dev.responsive.kafka.internal.db;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.mongo.KVDoc;
import dev.responsive.kafka.internal.db.mongo.MetadataDoc;
import dev.responsive.kafka.internal.db.mongo.MongoWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;

public class MongoKVFlushListener implements FlushListener<Bytes, Integer> {

  private final RemoteTable<Bytes, WriteModel<KVDoc>> table;
  private final MongoCollection<KVDoc> kvDocs;
  private final MongoCollection<MetadataDoc> metadataDocs;
  private final int kafkaPartition;
  private final TablePartitioner<Bytes, Integer> partitioner;

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(final Integer tablePartition) {
    return new MongoWriter<>(table, kafkaPartition, tablePartition, kvDocs);
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    return RemoteWriteResult.success(kafkaPartition);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {

    return null;
  }
}
