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

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.client.FindIterable;
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
import com.mongodb.client.result.UpdateResult;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.MongoKVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Utils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoKVTable implements RemoteKVTable<WriteModel<KVDoc>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoKVTable.class);
  private static final String KV_COLLECTION_NAME = "kv_data";
  private static final String METADATA_COLLECTION_NAME = "kv_metadata";

  private final String name;
  private final KeyCodec keyCodec;
  private final Optional<TtlResolver<?, ?>> ttlResolver;
  private final long defaultTtlMs;
  private final MongoCollection<KVDoc> docs;
  private final MongoCollection<KVMetadataDoc> metadata;

  private final ConcurrentMap<Integer, Long> kafkaPartitionToEpoch = new ConcurrentHashMap<>();

  public MongoKVTable(
      final MongoClient client,
      final String name,
      final CollectionCreationOptions collectionCreationOptions,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveConfig config
  ) {
    this.name = name;
    this.keyCodec = new StringKeyCodec();
    this.ttlResolver = ttlResolver;
    final CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    final CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoDatabase database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    if (collectionCreationOptions.sharded()) {
      final MongoDatabase adminDatabase = client.getDatabase("admin");
      docs = MongoUtils.createShardedCollection(
          KV_COLLECTION_NAME,
          KVDoc.class,
          database,
          adminDatabase,
          collectionCreationOptions.numChunks()
      );
    } else {
      docs = database.getCollection(KV_COLLECTION_NAME, KVDoc.class);
    }
    metadata = database.getCollection(METADATA_COLLECTION_NAME, KVMetadataDoc.class);

    // this is idempotent
    final long tombstoneRetentionSeconds =
        config.getLong(ResponsiveConfig.MONGO_TOMBSTONE_RETENTION_SEC_CONFIG);
    docs.createIndex(
        Indexes.descending(KVDoc.TOMBSTONE_TS),
        new IndexOptions().expireAfter(tombstoneRetentionSeconds, TimeUnit.SECONDS)
    );

    if (ttlResolver.isPresent()) {
      // TODO(sophie): account for infinite default ttl when we add row-level ttl
      if (!ttlResolver.get().hasDefaultOnly()) {
        throw new UnsupportedOperationException("Row-level ttl is not yet supported with MongoDB");
      }

      this.defaultTtlMs = ttlResolver.get().defaultTtl().toMillis();
      final long expireAfterMs =
          defaultTtlMs + Duration.ofSeconds(tombstoneRetentionSeconds).toMillis();

      docs.createIndex(
          Indexes.descending(KVDoc.TIMESTAMP),
          new IndexOptions().expireAfter(expireAfterMs, TimeUnit.MILLISECONDS)
      );
    } else {
      defaultTtlMs = 0L;
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MongoKVFlushManager init(final int kafkaPartition) {
    final KVMetadataDoc metaDoc = metadata.findOneAndUpdate(
        Filters.eq(KVMetadataDoc.PARTITION, kafkaPartition),
        Updates.combine(
            Updates.setOnInsert(KVMetadataDoc.PARTITION, kafkaPartition),
            Updates.setOnInsert(KVMetadataDoc.PARTITION, kafkaPartition),
            Updates.setOnInsert(KVMetadataDoc.OFFSET, NO_COMMITTED_OFFSET),
            Updates.inc(KVMetadataDoc.EPOCH, 1) // will set the value to 1 if it doesn't exist
        ),
        new FindOneAndUpdateOptions()
            .upsert(true)
            .returnDocument(ReturnDocument.AFTER)
    );

    if (metaDoc == null) {
      throw new IllegalStateException("Uninitialized metadata for partition " + kafkaPartition);
    }

    LOG.info("Retrieved initial metadata {}", metaDoc);

    kafkaPartitionToEpoch.put(kafkaPartition, metaDoc.epoch);
    return new MongoKVFlushManager(this, docs, kafkaPartition);
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long streamTimeMs) {
    final Date minValidTs = new Date(ttlResolver.isEmpty() ? -1L : streamTimeMs - defaultTtlMs);

    final KVDoc v = docs.find(Filters.and(
        Filters.eq(KVDoc.ID, keyCodec.encode(key)),
        Filters.gte(KVDoc.TIMESTAMP, minValidTs)
    )).first();
    return v == null ? null : v.getValue();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    final var toFilter = to == null ? null : Filters.lte(KVDoc.ID, keyCodec.encode(to));
    return doRange(kafkaPartition, from, toFilter, streamTimeMs);
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefix(
      final P prefix,
      final PS prefixKeySerializer,
      final int kafkaPartition,
      final long streamTimeMs
  ) {
    final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
    final Bytes to = Utils.incrementWithoutOverflow(from);

    final var toFilter = to == null ? null : Filters.lt(KVDoc.ID, keyCodec.encode(to));
    return doRange(kafkaPartition, from, toFilter, streamTimeMs);
  }

  private KeyValueIterator<Bytes, byte[]> doRange(
      final int kafkaPartition,
      final Bytes from,
      final Bson toFilter,
      final long streamTimeMs
  ) {
    final Date minValidTs = new Date(ttlResolver.isEmpty() ? -1L : streamTimeMs - defaultTtlMs);

    final var filters = new ArrayList<>(List.of(
        Filters.not(Filters.exists(KVDoc.TOMBSTONE_TS)),
        Filters.gte(KVDoc.TIMESTAMP, minValidTs),
        Filters.eq(KVDoc.KAFKA_PARTITION, kafkaPartition)
    ));

    if (from != null) {
      filters.add(Filters.gte(KVDoc.ID, keyCodec.encode(from)));
    }

    if (toFilter != null) {
      filters.add(toFilter);
    }

    final FindIterable<KVDoc> result = docs.find(Filters.and(filters));
    return Iterators.kv(
        result.iterator(),
        doc -> new KeyValue<>(
            keyCodec.decode(doc.getKey()),
            doc.getTombstoneTs() == null ? doc.getValue() : null
        ));
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long streamTimeMs) {
    final Date minValidTs = new Date(ttlResolver.isEmpty() ? -1L : streamTimeMs - defaultTtlMs);

    final FindIterable<KVDoc> result = docs.find(Filters.and(
        Filters.not(Filters.exists(KVDoc.TOMBSTONE_TS)),
        Filters.gte(KVDoc.TIMESTAMP, minValidTs),
        Filters.eq(KVDoc.KAFKA_PARTITION, kafkaPartition)
    ));
    return Iterators.kv(
        result.iterator(),
        doc -> new KeyValue<>(
            keyCodec.decode(doc.getKey()),
            doc.getTombstoneTs() == null ? doc.getValue() : null
        )
    );
  }

  @Override
  public WriteModel<KVDoc> insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long timestampMs
  ) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    final Date timestamp = new Date(timestampMs);
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(KVDoc.ID, keyCodec.encode(key)),
            Filters.lte(KVDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(KVDoc.VALUE, value),
            Updates.set(KVDoc.EPOCH, epoch),
            Updates.set(KVDoc.TIMESTAMP, timestamp),
            Updates.set(KVDoc.KAFKA_PARTITION, kafkaPartition),
            Updates.unset(KVDoc.TOMBSTONE_TS)
        ),
        new UpdateOptions().upsert(true)
    );
  }

  @Override
  public WriteModel<KVDoc> delete(final int kafkaPartition, final Bytes key) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(KVDoc.ID, keyCodec.encode(key)),
            Filters.lte(KVDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.unset(KVDoc.VALUE),
            Updates.unset(KVDoc.TIMESTAMP),
            Updates.set(KVDoc.TOMBSTONE_TS, new Date()),
            Updates.set(KVDoc.EPOCH, epoch)
        ),
        new UpdateOptions().upsert(true)
    );
  }

  @Override
  public long lastWrittenOffset(final int kafkaPartition) {
    final KVMetadataDoc result = metadata.find(
        Filters.eq(KVMetadataDoc.PARTITION, kafkaPartition)
    ).first();
    if (result == null) {
      throw new IllegalStateException("Expected to find metadata row");
    }
    return result.offset;
  }

  public UpdateResult setOffset(final int kafkaPartition, final long offset) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);

    return metadata.updateOne(
        Filters.and(
            Filters.eq(KVMetadataDoc.PARTITION, kafkaPartition),
            Filters.lte(KVMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(KVMetadataDoc.OFFSET, offset),
            Updates.set(KVMetadataDoc.EPOCH, epoch)
        )
    );
  }

  public long localEpoch(final int kafkaPartition) {
    return kafkaPartitionToEpoch.get(kafkaPartition);
  }

  public long fetchEpoch(final int kafkaPartition) {
    final KVMetadataDoc result = metadata.find(
        Filters.eq(KVMetadataDoc.PARTITION, kafkaPartition)
    ).first();
    if (result == null) {
      throw new IllegalStateException("Expected to find metadata row");
    }
    return result.epoch;
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    LOG.warn("approximateNumEntries is not yet implemented for Mongo");
    return 0;
  }
}
