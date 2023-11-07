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
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner.DefaultPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.List;
import org.bson.Document;

public class MongoWriterFactory<K> extends WriterFactory<K, Integer> {

  private final RemoteTable<K, WriteModel<Document>> table;
  private final MongoCollection<Document> genericDocs;
  private final MongoCollection<Document> genericMetadata;
  private final TablePartitioner<K, Integer> partitioner;
  private final int kafkaPartition;

  public MongoWriterFactory(
      final RemoteTable<K, WriteModel<Document>> table,
      final MongoCollection<Document> genericDocs,
      final MongoCollection<Document> genericMetadata,
      final int kafkaPartition
  ) {
    super(String.format("MongoWriterFactory [%s-%d] ", table.name(), kafkaPartition));
    this.table = table;
    this.genericDocs = genericDocs;
    this.genericMetadata = genericMetadata;
    this.partitioner = new DefaultPartitioner<>();
    this.kafkaPartition = kafkaPartition;
  }

  @Override
  public RemoteWriter<K, Integer> createWriter(
      final Integer tablePartition
  ) {
    return new MongoWriter<>(table, kafkaPartition, genericDocs);
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public Integer tablePartitionForKey(final K key) {
    return partitioner.tablePartition(kafkaPartition, key);
  }

  @Override
  public RemoteWriteResult<Integer> setOffset(final long consumedOffset) {
    genericMetadata.bulkWrite(List.of(table.setOffset(kafkaPartition, consumedOffset)));
    return RemoteWriteResult.success(kafkaPartition);
  }

  @Override
  protected long offset() {
    return table.fetchOffset(kafkaPartition);
  }
}
