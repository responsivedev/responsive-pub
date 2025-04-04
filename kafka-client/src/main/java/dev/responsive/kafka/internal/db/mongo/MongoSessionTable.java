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

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
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
import dev.responsive.kafka.internal.db.MongoSessionFlushManager;
import dev.responsive.kafka.internal.db.RemoteSessionTable;
import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.SessionKey;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSessionTable implements RemoteSessionTable<WriteModel<SessionDoc>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoSessionTable.class);
  private static final String METADATA_COLLECTION_NAME = "session_metadata";

  private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

  private final String name;
  private final SessionSegmentPartitioner partitioner;

  private final MongoDatabase database;
  private final MongoDatabase adminDatabase;
  private final MongoCollection<SessionMetadataDoc> metadata;
  private final CollectionCreationOptions collectionCreationOptions;
  private final KeyCodec keyCodec = new StringKeyCodec();

  private final ConcurrentMap<Integer, PartitionSegments> kafkaPartitionToSegments =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Long> kafkaPartitionToEpoch = new ConcurrentHashMap<>();

  private static class PartitionSegments {
    private final MongoDatabase database;
    private final MongoDatabase adminDatabase;
    private final Segmenter segmenter;
    private final long epoch;
    private final CollectionCreationOptions collectionCreationOptions;

    // Recommended to keep the total number of collections under 10,000, so we should not
    // let num_segments * num_kafka_partitions exceed 10k at the most
    private final Map<Segmenter.SegmentPartition, MongoCollection<SessionDoc>> segmentToCollection;

    public PartitionSegments(
        final MongoDatabase database,
        final MongoDatabase adminDatabase,
        final Segmenter segmenter,
        final int kafkaPartition,
        final long streamTime,
        final long epoch,
        final CollectionCreationOptions collectionCreationOptions
    ) {
      this.database = database;
      this.adminDatabase = adminDatabase;
      this.segmenter = segmenter;
      this.epoch = epoch;
      this.collectionCreationOptions = collectionCreationOptions;
      this.segmentToCollection = new ConcurrentHashMap<>();

      final var activeSegments = segmenter.activeSegments(kafkaPartition, streamTime);
      if (activeSegments.isEmpty()) {
        LOG.info("{}[{}] No active segments for initial streamTime {}",
                 database.getName(), kafkaPartition, streamTime);
      } else {
        for (final Segmenter.SegmentPartition segmentToCreate : activeSegments) {
          // Most likely if we have active segments upon initialization, the corresponding
          // collections and windowStart indices already exist, in which case #getCollection and
          // #createIndex should be quick and won't rebuild either structure if one already exists
          createSegment(segmentToCreate);
        }

        final long firstSegmentStartTimestamp = activeSegments.get(0).segmentStartTimestamp;
        final long lastSegmentStartTimestamp =
            firstSegmentStartTimestamp + (activeSegments.size() * segmenter.segmentIntervalMs());
        LOG.info(
            "{}[{}] Initialized active segments with start timestamps in [{} - {}]",
            database.getName(),
            kafkaPartition,
            firstSegmentStartTimestamp,
            lastSegmentStartTimestamp
        );
      }
    }

    private String collectionNameForSegment(final Segmenter.SegmentPartition segmentPartition) {
      final long segmentStartTimeMs =
          segmentPartition.segmentStartTimestamp * segmenter.segmentIntervalMs();
      return String.format(
          "%d-%d",
          segmentPartition.tablePartition, segmentStartTimeMs
      );
    }

    private void createSegment(final Segmenter.SegmentPartition segmentToCreate) {
      LOG.info(
          "{}[{}] Creating segment with start timestamp {}",
          database.getName(), segmentToCreate.tablePartition, segmentToCreate.segmentStartTimestamp
      );

      final var collectionName = collectionNameForSegment(segmentToCreate);
      final MongoCollection<SessionDoc> sessionDocs;
      if (collectionCreationOptions.sharded()) {
        sessionDocs = MongoUtils.createShardedCollection(
            collectionName,
            SessionDoc.class,
            database,
            adminDatabase,
            collectionCreationOptions.numChunks()
        );
      } else {
        sessionDocs = database.getCollection(
            collectionNameForSegment(segmentToCreate),
            SessionDoc.class
        );
      }
      segmentToCollection.put(segmentToCreate, sessionDocs);

      // TODO(agavra): make the tombstone retention configurable
      // this is idempotent
      sessionDocs.createIndex(
          Indexes.descending(SessionDoc.TOMBSTONE_TS),
          new IndexOptions().expireAfter(12L, TimeUnit.HOURS)
      );
    }

    // Note: it is always safe to drop a segment, even without validating the epoch, since we only
    // ever attempt to expire segments after successfully flushing the batch which rolled them,
    // thus guaranteeing that even if we are fenced, the new writer will pick up from an offset and
    // stream-time such that the segment is already expired
    // However this does leave us vulnerable to zombie segments. We should consider doing a sweep
    // for old segments every so often with #listCollectionNames, since the segmentId is embedded in
    // the collection name and will indicate whether it's part of the active set or not, perhaps
    // using a scheduled executor to avoid blocking processing
    // In fact we may want to move all the segment expiration to a background process since there's
    // no async way to drop a collection but the stream thread doesn't actually need to wait for it
    private void deleteSegment(final Segmenter.SegmentPartition segmentToExpire) {
      LOG.info(
          "{}[{}] Expiring segment with start timestamp {}",
          database.getName(), segmentToExpire.tablePartition, segmentToExpire.segmentStartTimestamp
      );

      final var expiredDocs = segmentToCollection.get(segmentToExpire);
      expiredDocs.drop();
    }
  }

  public MongoSessionTable(
      final MongoClient client,
      final String name,
      final SessionSegmentPartitioner partitioner,
      final CollectionCreationOptions collectionCreationOptions
  ) {
    this.name = name;
    this.partitioner = partitioner;
    this.collectionCreationOptions = collectionCreationOptions;

    final CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    final CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    this.database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    this.adminDatabase = client.getDatabase("admin");
    this.metadata = database.getCollection(
        METADATA_COLLECTION_NAME,
        SessionMetadataDoc.class
    );
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MongoSessionFlushManager init(
      final int kafkaPartition
  ) {
    final SessionMetadataDoc metaDoc = metadata.findOneAndUpdate(
        Filters.eq(SessionMetadataDoc.PARTITION, kafkaPartition),
        Updates.combine(
            Updates.setOnInsert(SessionMetadataDoc.PARTITION, kafkaPartition),
            Updates.setOnInsert(SessionMetadataDoc.OFFSET, NO_COMMITTED_OFFSET),
            Updates.setOnInsert(
                SessionMetadataDoc.STREAM_TIME,
                Segmenter.UNINITIALIZED_STREAM_TIME
            ),
            Updates.inc(SessionMetadataDoc.EPOCH, 1) // will set the value to 1 if it doesn't exist
        ),
        new FindOneAndUpdateOptions()
            .upsert(true)
            .returnDocument(ReturnDocument.AFTER)
    );

    if (metaDoc == null) {
      throw new IllegalStateException("Uninitialized metadata for partition " + kafkaPartition);
    }

    LOG.info("{}[{}] Retrieved initial metadata {}", name, kafkaPartition, metaDoc);

    kafkaPartitionToSegments.put(
        kafkaPartition,
        new PartitionSegments(
            database,
            adminDatabase,
            partitioner.segmenter(),
            kafkaPartition,
            metaDoc.streamTime,
            metaDoc.epoch,
            collectionCreationOptions
        )
    );
    kafkaPartitionToEpoch.put(kafkaPartition, metaDoc.epoch);

    return new MongoSessionFlushManager(
        this,
        (segment) -> collectionForSegmentPartition(kafkaPartition, segment),
        partitioner,
        kafkaPartition,
        metaDoc.streamTime
    );
  }

  /**
   * Inserts data into this table. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param kafkaPartition the kafka partition
   * @param sessionKey     the windowed data key
   * @param value          the data value
   * @param timestampMs    the timestamp of the event
   * @return a statement that, when executed, will insert the value
   * for this key and partition into the table.
   * Note that the key in this case is a "session" key, where
   * {@code sessionKey} includes both the record key and
   * the session start / end timestamps
   */
  @Override
  public WriteModel<SessionDoc> insert(
      final int kafkaPartition,
      final SessionKey sessionKey,
      final byte[] value,
      final long timestampMs
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    final BasicDBObject id = compositeKey(sessionKey);
    final long epoch = partitionSegments.epoch;
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(SessionDoc.ID, id),
            Filters.lte(SessionDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(SessionDoc.VALUE, value),
            Updates.set(SessionDoc.EPOCH, epoch),
            Updates.unset(SessionDoc.TOMBSTONE_TS)
        ),
        UPSERT_OPTIONS
    );
  }

  /**
   * @param kafkaPartition the kafka partition
   * @param sessionKey     the session data key
   * @return a statement that, when executed, will delete the value
   * for this key and partition in the table.
   * Note that the key in this case is a "session" key, where
   * {@code sessionKey} includes both the record key and
   * the sessionStart / sessionEnd timestamps
   */
  @Override
  public WriteModel<SessionDoc> delete(
      final int kafkaPartition,
      final SessionKey sessionKey
  ) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(SessionDoc.ID, compositeKey(sessionKey)),
            Filters.lte(SessionDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.unset(SessionDoc.VALUE),
            Updates.set(SessionDoc.TOMBSTONE_TS, Date.from(Instant.now())),
            Updates.set(SessionDoc.EPOCH, epoch)
        ),
        UPSERT_OPTIONS
    );
  }

  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long sessionStart,
      final long sessionEnd
  ) {
    final SessionKey sessionKey = new SessionKey(key, sessionStart, sessionEnd);
    final Segmenter.SegmentPartition segment =
        partitioner.tablePartition(kafkaPartition, sessionKey);
    final var segmentSessions = collectionForSegmentPartition(kafkaPartition, segment);
    if (segmentSessions == null) {
      return null;
    }

    final SessionDoc sessionDoc = segmentSessions.find(
            Filters.and(
                Filters.eq(SessionDoc.ID, compositeKey(sessionKey))
            ))
        .first();

    return sessionDoc == null ? null : sessionDoc.value();
  }

  public KeyValueIterator<SessionKey, byte[]> fetchAll(
      final int kafkaPartition,
      final Bytes key,
      final long earliestSessionEnd,
      final long latestSessionEnd
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    final var candidateSegments = partitioner.segmenter().range(
        kafkaPartition,
        earliestSessionEnd,
        latestSessionEnd
    );

    final var minKey = new SessionKey(key, 0, earliestSessionEnd);
    final var maxKey = new SessionKey(key, Long.MAX_VALUE, latestSessionEnd);
    final List<KeyValueIterator<SessionKey, byte[]>> segmentIterators = new LinkedList<>();

    for (final var segment : candidateSegments) {
      final var collection = partitionSegments.segmentToCollection.get(segment);
      if (collection == null) {
        LOG.warn(
            "Session segment collection could not be queried: {}",
            segment.segmentStartTimestamp
        );
        continue;
      }

      final FindIterable<SessionDoc> fetchResults = collection.find(
          Filters.and(
              Filters.gte(SessionDoc.ID, compositeKey(minKey)),
              Filters.lte(SessionDoc.ID, compositeKey(maxKey)),
              Filters.exists(SessionDoc.TOMBSTONE_TS, false)
          )
      );

      final KeyValueIterator<SessionKey, byte[]> iterator =
          Iterators.kv(fetchResults.iterator(), this::sessionFromDoc);
      segmentIterators.add(iterator);
    }

    return Iterators.wrapped(segmentIterators);
  }

  @VisibleForTesting
  MongoCollection<SessionDoc> collectionForSegmentPartition(
      final int kafkaPartition,
      final Segmenter.SegmentPartition segment
  ) {
    return kafkaPartitionToSegments.get(kafkaPartition).segmentToCollection.get(segment);
  }

  public RemoteWriteResult<Segmenter.SegmentPartition> createSegmentForPartition(
      final int kafkaPartition,
      final Segmenter.SegmentPartition segmentPartition
  ) {
    kafkaPartitionToSegments.get(kafkaPartition).createSegment(segmentPartition);
    return RemoteWriteResult.success(segmentPartition);
  }

  public RemoteWriteResult<Segmenter.SegmentPartition> deleteSegmentForPartition(
      final int kafkaPartition,
      final Segmenter.SegmentPartition segmentPartition
  ) {
    kafkaPartitionToSegments.get(kafkaPartition).deleteSegment(segmentPartition);
    return RemoteWriteResult.success(segmentPartition);
  }

  public long localEpoch(final int kafkaPartition) {
    return kafkaPartitionToSegments.get(kafkaPartition).epoch;
  }

  public long fetchEpoch(final int kafkaPartition) {
    final SessionMetadataDoc remoteMetadata = metadata.find(
        Filters.eq(SessionMetadataDoc.PARTITION, kafkaPartition)
    ).first();

    if (remoteMetadata == null) {
      LOG.error("{}[{}] Epoch fetch failed due to missing metadata row",
                name, kafkaPartition);
      throw new IllegalStateException("No metadata row found for partition " + kafkaPartition);
    }
    return remoteMetadata.epoch;
  }

  @Override
  public long lastWrittenOffset(final int kafkaPartition) {
    final SessionMetadataDoc remoteMetadata = metadata.find(
        Filters.eq(SessionMetadataDoc.PARTITION, kafkaPartition)
    ).first();

    if (remoteMetadata == null) {
      LOG.error("{}[{}] Offset fetch failed due to missing metadata row",
                name, kafkaPartition);
      throw new IllegalStateException("No metadata row found for partition " + kafkaPartition);
    }

    final long localEpoch = kafkaPartitionToSegments.get(kafkaPartition).epoch;
    if (remoteMetadata.epoch > localEpoch) {
      LOG.warn("{}[{}] Fenced retrieving start offset due to stored epoch {} being greater "
                   + "than local epoch {} ",
               name, kafkaPartition, remoteMetadata.epoch, localEpoch);
      throw new TaskMigratedException("Fenced while fetching offset to start restoration from");
    }

    return remoteMetadata.offset;
  }

  public UpdateResult setOffsetAndStreamTime(
      final int kafkaPartition,
      final long offset,
      final long streamTime
  ) {
    final var segments = kafkaPartitionToSegments.get(kafkaPartition);
    final long epoch = segments.epoch;

    LOG.info("{}[{}] Updating offset to {} and streamTime to {} with epoch {}",
             name, kafkaPartition, offset, streamTime, epoch);

    return metadata.updateOne(
        Filters.and(
            Filters.eq(SessionMetadataDoc.PARTITION, kafkaPartition),
            Filters.lte(SessionMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(SessionMetadataDoc.OFFSET, offset),
            Updates.set(SessionMetadataDoc.STREAM_TIME, streamTime),
            Updates.set(SessionMetadataDoc.EPOCH, epoch)
        )
    );
  }

  public BasicDBObject compositeKey(final SessionKey sessionKey) {
    return SessionDoc.compositeKey(keyCodec.encode(sessionKey.key), sessionKey.sessionStartMs,
        sessionKey.sessionEndMs
    );
  }

  private KeyValue<SessionKey, byte[]> sessionFromDoc(final SessionDoc sessionDoc) {
    return new KeyValue<>(
        new SessionKey(
            keyCodec.decode(sessionDoc.unwrapRecordKey()),
            sessionDoc.unwrapSessionStartMs(),
            sessionDoc.unwrapSessionEndMs()
        ),
        sessionDoc.value()
    );
  }
}

