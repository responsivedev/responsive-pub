/*
 *
 *  Copyright 2023 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db.mongo;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.internal.db.partitioning.Segmenter.UNINITIALIZED_STREAM_TIME;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import dev.responsive.kafka.internal.db.MongoWindowFlushManager;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWindowedTable implements RemoteWindowedTable<WriteModel<WindowDoc>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoWindowedTable.class);
  private static final String METADATA_COLLECTION_NAME = "window_metadata";

  private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

  private final String name;
  private final WindowSegmentPartitioner partitioner;

  // whether to put windowStartMs first in the composite windowed key format in WindowDoc
  private final boolean timestampFirstOrder;

  private final MongoDatabase database;
  private final MongoDatabase adminDatabase;
  private final MongoCollection<WindowMetadataDoc> metadata;
  private final ConcurrentMap<Integer, PartitionSegments> kafkaPartitionToSegments =
      new ConcurrentHashMap<>();
  private final CollectionCreationOptions collectionCreationOptions;

  private static class PartitionSegments {
    private final MongoDatabase database;
    private final MongoDatabase adminDatabase;
    private final Segmenter segmenter;
    private final long epoch;
    private final CollectionCreationOptions collectionCreationOptions;

    // Recommended to keep the total number of collections under 10,000, so we should not
    // let num_segments * num_kafka_partitions exceed 10k at the most
    private final Map<SegmentPartition, MongoCollection<WindowDoc>> segmentWindows;

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
      this.segmentWindows = new ConcurrentHashMap<>();

      final List<SegmentPartition> activeSegments =
          segmenter.activeSegments(kafkaPartition, streamTime);
      if (activeSegments.isEmpty()) {
        LOG.info("{}[{}] No active segments for initial streamTime {}",
                 database.getName(), kafkaPartition, streamTime);
      } else {
        for (final SegmentPartition segmentToCreate : activeSegments) {
          // Most likely if we have active segments upon initialization, the corresponding
          // collections and windowStart indices already exist, in which case #getCollection and
          // #createIndex should be quick and won't rebuild either structure if one already exists
          createSegment(segmentToCreate);
        }

        final long firstSegmentId = activeSegments.get(0).segmentId;
        LOG.info("{}[{}] Initialized active segments in range {} - {}",
                 database.getName(), kafkaPartition, firstSegmentId,
                 firstSegmentId + activeSegments.size());
      }
    }

    private String collectionNameForSegment(final SegmentPartition segmentPartition) {
      final long segmentStartTimeMs = segmentPartition.segmentId * segmenter.segmentIntervalMs();
      return String.format(
          "%d-%d",
          segmentPartition.tablePartition, segmentStartTimeMs
      );
    }

    private void createSegment(final SegmentPartition segmentToCreate) {
      LOG.info("{}[{}] Creating segment id {}",
               database.getName(), segmentToCreate.tablePartition, segmentToCreate.segmentId);

      final var collectionName = collectionNameForSegment(segmentToCreate);
      final MongoCollection<WindowDoc> windowDocs;
      if (collectionCreationOptions.sharded()) {
        windowDocs = MongoUtils.createShardedCollection(
            collectionName,
            WindowDoc.class,
            database,
            adminDatabase,
            collectionCreationOptions.numChunks()
        );
      } else {
        windowDocs = database.getCollection(
            collectionNameForSegment(segmentToCreate),
            WindowDoc.class
        );
      }
      segmentWindows.put(segmentToCreate, windowDocs);
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
    private void deleteSegment(final SegmentPartition segmentToExpire) {
      LOG.info("{}[{}] Expiring segment id {}",
               database.getName(), segmentToExpire.tablePartition, segmentToExpire.segmentId);

      final var expiredDocs = segmentWindows.get(segmentToExpire);
      expiredDocs.drop();
    }
  }

  public MongoWindowedTable(
      final MongoClient client,
      final String name,
      final WindowSegmentPartitioner partitioner,
      final boolean timestampFirstOrder,
      final CollectionCreationOptions collectionCreationOptions
  ) {
    this.name = name;
    this.partitioner = partitioner;
    this.timestampFirstOrder = timestampFirstOrder;
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
        WindowMetadataDoc.class
    );
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MongoWindowFlushManager init(
      final int kafkaPartition
  ) {
    final WindowMetadataDoc metaDoc = metadata.findOneAndUpdate(
        Filters.eq(WindowMetadataDoc.PARTITION, kafkaPartition),
        Updates.combine(
            Updates.setOnInsert(WindowMetadataDoc.PARTITION, kafkaPartition),
            Updates.setOnInsert(WindowMetadataDoc.OFFSET, NO_COMMITTED_OFFSET),
            Updates.setOnInsert(WindowMetadataDoc.STREAM_TIME, UNINITIALIZED_STREAM_TIME),
            Updates.inc(WindowMetadataDoc.EPOCH, 1) // will set the value to 1 if it doesn't exist
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

    return new MongoWindowFlushManager(
        this,
        (segment) -> windowsForSegmentPartition(kafkaPartition, segment),
        partitioner,
        kafkaPartition,
        metaDoc.streamTime
    );
  }

  private MongoCollection<WindowDoc> windowsForSegmentPartition(
      final int kafkaPartition,
      final SegmentPartition segment
  ) {
    return kafkaPartitionToSegments.get(kafkaPartition).segmentWindows.get(segment);
  }

  public RemoteWriteResult<SegmentPartition> createSegmentForPartition(
      final int kafkaPartition,
      final SegmentPartition segmentPartition
  ) {
    kafkaPartitionToSegments.get(kafkaPartition).createSegment(segmentPartition);
    return RemoteWriteResult.success(segmentPartition);
  }

  public RemoteWriteResult<SegmentPartition> deleteSegmentForPartition(
      final int kafkaPartition,
      final SegmentPartition segmentPartition
  ) {
    kafkaPartitionToSegments.get(kafkaPartition).deleteSegment(segmentPartition);
    return RemoteWriteResult.success(segmentPartition);
  }

  public long localEpoch(final int kafkaPartition) {
    return kafkaPartitionToSegments.get(kafkaPartition).epoch;
  }

  public long fetchEpoch(final int kafkaPartition) {
    final WindowMetadataDoc remoteMetadata = metadata.find(
        Filters.eq(WindowMetadataDoc.PARTITION, kafkaPartition)
    ).first();

    if (remoteMetadata == null) {
      LOG.error("{}[{}] Epoch fetch failed due to missing metadata row",
                name, kafkaPartition);
      throw new IllegalStateException("No metadata row found for partition " + kafkaPartition);
    }
    return remoteMetadata.epoch;
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final WindowMetadataDoc remoteMetadata = metadata.find(
        Filters.eq(WindowMetadataDoc.PARTITION, kafkaPartition)
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
            Filters.eq(WindowMetadataDoc.PARTITION, kafkaPartition),
            Filters.lte(WindowMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowMetadataDoc.OFFSET, offset),
            Updates.set(WindowMetadataDoc.STREAM_TIME, streamTime),
            Updates.set(WindowMetadataDoc.EPOCH, epoch)
        )
    );
  }

  /**
   * Inserts data into this table. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param kafkaPartition the kafka partition
   * @param windowedKey    the windowed data key
   * @param value          the data value
   * @param epochMillis    the timestamp of the event
   * @return a statement that, when executed, will insert the value
   *         for this key and partition into the table.
   *         Note that the key in this case is a "windowed" key, where
   *         {@code windowedKey} includes both the record key and
   *         the windowStart timestamp
   */
  @Override
  public WriteModel<WindowDoc> insert(
      final int kafkaPartition,
      final WindowedKey windowedKey,
      final byte[] value,
      final long epochMillis
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    final long epoch = partitionSegments.epoch;
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowDoc.ID, compositeKey(windowedKey)),
            Filters.lte(WindowDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowDoc.VALUE, value),
            Updates.set(WindowDoc.EPOCH, epoch)
        ),
        UPSERT_OPTIONS
    );
  }

  /**
   * @param kafkaPartition  the kafka partition
   * @param windowedKey    the windowed data key
   *
   * @return a statement that, when executed, will delete the value
   *         for this key and partition in the table.
   *         Note that the key in this case is a "windowed" key, where
   *         {@code windowedKey} includes both the record key and
   *         the windowStart timestamp
   */
  @Override
  public WriteModel<WindowDoc> delete(
      final int kafkaPartition,
      final WindowedKey windowedKey
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);
    throw new UnsupportedOperationException("Deletes not yet supported for MongoDB window stores");
  }

  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long windowStart
  ) {
    final WindowedKey windowedKey = new WindowedKey(key, windowStart);
    final var segment = partitioner.tablePartition(
        kafkaPartition,
        windowedKey
    );
    final var segmentWindows = windowsForSegmentPartition(kafkaPartition, segment);
    if (segmentWindows == null) {
      return null;
    }

    final WindowDoc windowDoc = segmentWindows.find(
        Filters.and(
            Filters.eq(WindowDoc.ID, compositeKey(windowedKey))))
        .first();
    return windowDoc == null ? null : windowDoc.getValue();
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    for (final var segment : partitioner.segmenter().range(kafkaPartition, timeFrom, timeTo)) {
      final var segmentWindows = partitionSegments.segmentWindows.get(segment);
      final FindIterable<WindowDoc> fetchResults = segmentWindows.find(
          Filters.and(
              Filters.gte(WindowDoc.ID, compositeKey(key, timeFrom)),
              Filters.lte(WindowDoc.ID, compositeKey(key, timeTo))));

      segmentIterators.add(
          Iterators.kv(fetchResults.iterator(), MongoWindowedTable::windowFromDoc)
      );
    }
    return Iterators.wrapped(segmentIterators);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    // TODO: Mongo supports efficient reverse iteration if you set up a descending index
    //  We could expose an API for this so users can configure the store optimally if they
    //  plan to utilize any of the backwards fetches
    throw new UnsupportedOperationException("backFetch not yet supported for MongoDB backends");
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("fetchRange not yet supported for Mongo backends");

  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("backFetchRange not yet supported for Mongo backends");
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("fetchAll not yet supported for Mongo backends");
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("backFetchAll not yet supported for MongoDB backends");
  }

  public BasicDBObject compositeKey(final WindowedKey windowedKey) {
    return compositeKey(windowedKey.key, windowedKey.windowStartMs);
  }

  public BasicDBObject compositeKey(final Bytes key, final long windowStartMs) {
    return WindowDoc.compositeKey(
        key.get(),
        windowStartMs,
        timestampFirstOrder
    );
  }

  private static KeyValue<WindowedKey, byte[]> windowFromDoc(final WindowDoc windowDoc) {
    return new KeyValue<>(WindowDoc.windowedKey(windowDoc.id), windowDoc.value);
  }

}

