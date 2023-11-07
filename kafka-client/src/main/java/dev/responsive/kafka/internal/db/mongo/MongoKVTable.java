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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.WriterFactory;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoKVTable implements RemoteKVTable<WriteModel<Document>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoKVTable.class);

  private final String name;
  private final MongoCollection<KVDoc> collection;
  private final MongoCollection<MetadataDoc> metadata;

  // this map contains the initialized value of the metadata document,
  // for which the epoch and object ID will never change. it is likely
  // that after a few writes to MongoDB the cached MetadataDoc.offset value
  // will be out of date, so it should not be used beyond initialization
  private final ConcurrentMap<Integer, MetadataDoc> metadataRows = new ConcurrentHashMap<>();
  private final MongoCollection<Document> generic;

  public MongoKVTable(final MongoClient client, final String name) {
    this.name = name;
    CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoDatabase database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    generic = database.getCollection(name);
    collection = database.getCollection(name, KVDoc.class);
    metadata = database.getCollection(name, MetadataDoc.class);

    // TODO(agavra): make the tombstone retention configurable
    // this is idempotent
    collection.createIndex(
        Indexes.descending(KVDoc.TOMBSTONE_TS),
        new IndexOptions().expireAfter(12L, TimeUnit.HOURS)
    );
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<Bytes, Integer> init(final int kafkaPartition) {
    metadataRows.computeIfAbsent(kafkaPartition, kp -> {
      final MetadataDoc metaDoc = metadata.findOneAndUpdate(
          Filters.eq(MetadataDoc.PARTITION, kafkaPartition),
          Updates.combine(
              Updates.setOnInsert(MetadataDoc.PARTITION, kafkaPartition),
              Updates.setOnInsert(MetadataDoc.OFFSET, NO_COMMITTED_OFFSET),
              Updates.inc(MetadataDoc.EPOCH, 1) // will set the value to 1 if it doesn't exist
          ),
          new FindOneAndUpdateOptions()
              .upsert(true)
              .returnDocument(ReturnDocument.AFTER)
      );

      if (metaDoc == null) {
        throw new IllegalStateException("Uninitialized metadata for partition " + kafkaPartition);
      }

      LOG.info("Retrieved initial metadata {}", metaDoc);

      return metaDoc;
    });

    return new MongoWriterFactory<>(this, generic, kafkaPartition);
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long minValidTs) {
    final KVDoc v = collection.find(Filters.eq(KVDoc.ID, key.get())).first();
    return v == null ? null : v.getValue();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final int kafkaPartition, final Bytes from,
                                               final Bytes to,
                                               final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public WriteModel<Document> insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    final long epoch = metadataRows.get(kafkaPartition).epoch;
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(KVDoc.ID, key.get()),
            Filters.lte(KVDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(KVDoc.VALUE, value),
            Updates.set(KVDoc.EPOCH, epoch),
            Updates.unset(KVDoc.TOMBSTONE_TS)
        ),
        new UpdateOptions().upsert(true)
    );
  }

  @Override
  public WriteModel<Document> delete(final int kafkaPartition, final Bytes key) {
    final long epoch = metadataRows.get(kafkaPartition).epoch;
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(KVDoc.ID, key.get()),
            Filters.lte(KVDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.unset(KVDoc.VALUE),
            Updates.set(KVDoc.TOMBSTONE_TS, Date.from(Instant.now())),
            Updates.set(KVDoc.EPOCH, epoch)
        ),
        new UpdateOptions().upsert(true)
    );
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final MetadataDoc result = metadata.find(
        Filters.eq(MetadataDoc.ID, metadataRows.get(kafkaPartition).id())
    ).first();
    if (result == null) {
      throw new IllegalStateException("Expected to find metadata row");
    }
    return result.offset;
  }

  @Override
  public WriteModel<Document> setOffset(final int kafkaPartition, final long offset) {
    final long epoch = metadataRows.get(kafkaPartition).epoch;
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(MetadataDoc.ID, metadataRows.get(kafkaPartition).id()),
            Filters.lte(MetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(MetadataDoc.OFFSET, offset),
            Updates.set(MetadataDoc.EPOCH, epoch)
        )
    );
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    return 0;
  }

}
