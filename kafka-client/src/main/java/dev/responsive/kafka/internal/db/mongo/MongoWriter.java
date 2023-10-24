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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.bson.Document;

public class MongoWriter<K> implements RemoteWriter<K, Integer> {

  private final RemoteTable<K, WriteModel<Document>> table;
  private final int kafkaPartition;
  private final MongoCollection<Document> collection;
  private final List<WriteModel<Document>> accumulatedWrites = new ArrayList<>();

  public MongoWriter(
      final RemoteTable<K, WriteModel<Document>> table,
      final int kafkaPartition,
      final MongoCollection<Document> collection
  ) {
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.collection = collection;
  }

  @Override
  public void insert(final K key, final byte[] value, final long epochMillis) {
    accumulatedWrites.add(table.insert(kafkaPartition, key, value, epochMillis));
  }

  @Override
  public void delete(final K key) {
    accumulatedWrites.add(table.delete(kafkaPartition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult<Integer>> flush() {
    collection.bulkWrite(accumulatedWrites);
    accumulatedWrites.clear();
    return CompletableFuture.completedFuture(RemoteWriteResult.success(kafkaPartition));
  }

}
