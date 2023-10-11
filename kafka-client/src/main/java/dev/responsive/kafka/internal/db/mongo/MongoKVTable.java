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

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import dev.responsive.kafka.internal.db.MetadataRow;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;

public class MongoKVTable implements RemoteKVTable {

  private final String name;
  private final MongoCollection<KVDoc> collection;
  private final MongoCollection<MetadataDoc> metadata;

  private final ConcurrentMap<Integer, ObjectId> metadataRows = new ConcurrentHashMap<>();

  public MongoKVTable(final MongoClient client, final String name) {
    this.name = name;
    CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoDatabase database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    collection = database.getCollection(name, KVDoc.class);
    metadata = database.getCollection(name, MetadataDoc.class);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<Bytes> init(final SubPartitioner partitioner, final int kafkaPartition) {
    metadataRows.computeIfAbsent(kafkaPartition, kp -> {
      final UpdateResult updateResult = metadata.updateOne(
          Filters.eq("partition", kafkaPartition),
          Updates.setOnInsert("offset", NO_COMMITTED_OFFSET),
          new UpdateOptions().upsert(true));

      if (updateResult.getUpsertedId() != null) {
        return updateResult.getUpsertedId().asObjectId().getValue();
      }

      // find existing one
      final MetadataDoc metadataPartition = metadata.find(
          Filters.eq("partition", kafkaPartition)
      ).first();
      if (metadataPartition == null) {
        throw new IllegalStateException("No metadata partition despite upsert");
      }
      return metadataPartition.id();
    });

    return new MongoWriterFactory<>(this);
  }

  @Override
  public byte[] get(final int partition, final Bytes key, final long minValidTs) {
    final KVDoc v = collection.find(Filters.eq("key", key.get())).first();
    return v == null ? null : v.getValue();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final int partition, final Bytes from,
                                               final Bytes to,
                                               final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int partition, final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BoundStatement insert(
      final int partitionKey,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    collection.updateOne(
        Filters.eq("key", key.get()),
        Updates.set("value", value),
        new UpdateOptions().upsert(true));
    return null;
  }

  @Override
  public BoundStatement delete(final int partitionKey, final Bytes key) {
    collection.deleteOne(Filters.eq("key", key));
    return null;
  }

  @Override
  public MetadataRow metadata(final int partition) {
    final MetadataDoc result = metadata.find(
        Filters.eq("_id", metadataRows.get(partition))
    ).first();
    if (result == null) {
      throw new IllegalStateException("Expected to find metadata row");
    }
    return new MetadataRow(result.offset, -1L);
  }

  @Override
  public BoundStatement setOffset(final int partition, final long offset) {
    final UpdateResult modifiedCount = metadata.updateOne(
        Filters.eq("_id", metadataRows.get(partition)),
        Updates.set("offset", offset)
    );
    return null;
  }

  @Override
  public long approximateNumEntries(final int partition) {
    return 0;
  }

}
