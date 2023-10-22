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
import dev.responsive.kafka.internal.utils.SessionClients;
import org.bson.Document;

public class MongoWriterFactory<K> implements WriterFactory<K> {

  private final RemoteTable<K, WriteModel<Document>> table;
  private final MongoCollection<Document> genericCollection;

  public MongoWriterFactory(
      final RemoteTable<K, WriteModel<Document>> table,
      final MongoCollection<Document> genericCollection
  ) {
    this.table = table;
    this.genericCollection = genericCollection;
  }

  @Override
  public RemoteWriter<K> createWriter(
      final SessionClients client,
      final int partition
  ) {
    return new MongoWriter<>(table, partition, genericCollection);
  }

}
