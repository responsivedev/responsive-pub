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

package dev.responsive.kafka.internal.db.mongo;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.internal.db.mongo.WindowDoc.compositeKey;
import static dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.UNINITIALIZED_STREAM_TIME;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.MongoWindowFlushManager;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentRoll;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.SegmentBatch;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWindowedTable implements RemoteWindowedTable<WriteModel<WindowDoc>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoWindowedTable.class);
  private static final String METADATA_COLLECTION_SUFFIX = "_md";

  private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

  private final String name;
  private final SegmentPartitioner partitioner;

  private final MongoDatabase database;
  private final MongoCollection<WindowMetadataDoc> metadata;
  private final ConcurrentMap<Integer, PartitionSegments> kafkaPartitionToSegments =
      new ConcurrentHashMap<>();

  private class PartitionSegments {
    private final long epoch;
    private final SegmentBatch segmentBatch;

    // Recommended to keep the total number of collections under 10,000, so we should not
    // let num_segments * num_kafka_partitions exceed 10k at the most
    private final Map<SegmentPartition, MongoCollection<WindowDoc>> segmentWindows;

    public PartitionSegments(
        final long streamTime,
        final long epoch
    ) {
      this.segmentBatch = new SegmentBatch(streamTime);
      this.epoch = epoch;
      this.segmentWindows = new ConcurrentHashMap<>();
    }

    void createSegment(final SegmentPartition segmentToCreate) {
      LOG.info("{}[{}] Creating segment id {}",
               name, segmentToCreate.tablePartition, segmentToCreate.segmentId);

      final MongoCollection<WindowDoc> windowDocs =
          database.getCollection(collectionNameForSegment(segmentToCreate), WindowDoc.class);
      windowDocs.createIndex(
          Indexes.ascending(WindowDoc.WINDOW_START_TS)
      );

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
    void expireSegment(final SegmentPartition segmentToDelete) {
      LOG.info("{}[{}] Expiring segment id {}",
               name, segmentToDelete.tablePartition, segmentToDelete.segmentId);

      final var expiredDocs = segmentWindows.get(segmentToDelete);
      expiredDocs.drop();
    }
  }

  public MongoWindowedTable(
      final MongoClient client,
      final String name,
      final SegmentPartitioner partitioner
  ) {
    this.name = name;
    this.partitioner = partitioner;

    final CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    final CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    this.database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    this.metadata = database.getCollection(
        name + METADATA_COLLECTION_SUFFIX,
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
        Filters.eq(WindowMetadataDoc.ID, kafkaPartition),
        Updates.combine(
            Updates.setOnInsert(WindowMetadataDoc.ID, kafkaPartition),
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

    final var partitionSegments = new PartitionSegments(metaDoc.streamTime, metaDoc.epoch);

    final var activeSegments = partitioner.activeSegments(kafkaPartition, metaDoc.streamTime);
    if (activeSegments.isEmpty()) {
      LOG.info("{}[{}] No active segments for initial streamTime {}",
               name, kafkaPartition, metaDoc.streamTime);
    } else {
      for (final SegmentPartition segmentToCreate : activeSegments) {
        // Most likely if we have active segments upon initialization, the corresponding
        // collections and windowStart indices already exist, in which case #getCollection and
        // #createIndex should be quick and won't rebuild either structure if one already exists
        // TODO: optimize by using listCollectionNames to skip redundant collection/index creation
        //  since we should almost always have an index for each collection for all active segments
        partitionSegments.createSegment(segmentToCreate);
      }

      final long firstSegmentId = activeSegments.get(0).segmentId;
      LOG.info("{}[{}] Initialized active segments in range {} - {}",
               name, kafkaPartition, firstSegmentId, firstSegmentId + activeSegments.size());
    }

    kafkaPartitionToSegments.put(
        kafkaPartition,
        partitionSegments
    );

    return new MongoWindowFlushManager(
        this,
        (segment) -> windowsForSegmentPartition(kafkaPartition, segment),
        metadata,
        partitioner,
        kafkaPartition
    );
  }

  public String collectionNameForSegment(final SegmentPartition segmentPartition) {
    return String.format(
        "%s-%d-%d",
        name, segmentPartition.tablePartition, segmentPartition.segmentId
    );
  }

  public MongoCollection<WindowDoc> windowsForSegmentPartition(
      final int kafkaPartition,
      final SegmentPartition segment
  ) {
    return kafkaPartitionToSegments.get(kafkaPartition).segmentWindows.get(segment);
  }

  public RemoteWriteResult<SegmentPartition> preCommit(
      final int kafkaPartition
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    final var segmentBatch = partitionSegments.segmentBatch;
    final SegmentRoll pendingRoll = partitioner.rolledSegments(
        name, segmentBatch.flushedStreamTime, segmentBatch.batchStreamTime
    );

    segmentBatch.prepareRoll(pendingRoll);

    for (final long segmentId : pendingRoll.segmentsToCreate) {
      partitionSegments.createSegment(new SegmentPartition(kafkaPartition, segmentId));
    }
    return RemoteWriteResult.success(null);
  }

  public RemoteWriteResult<SegmentPartition> postCommit(
      final int kafkaPartition
  ) {
    final var segments = kafkaPartitionToSegments.get(kafkaPartition);

    for (final long segmentId : segments.segmentBatch.segmentRoll.segmentsToExpire) {
      segments.expireSegment(new SegmentPartition(kafkaPartition, segmentId));
    }
    segments.segmentBatch.finalizeRoll();
    return RemoteWriteResult.success(null);
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final WindowMetadataDoc result = metadata.find(
        Filters.eq(WindowMetadataDoc.ID, kafkaPartition)
    ).first();
    if (result == null) {
      LOG.error("Offset fetch failed due to missing metadata row for partition {}", kafkaPartition);
      throw new IllegalStateException("No metadata row found for partition " + kafkaPartition);
    }
    return result.offset;
  }

  public WriteModel<WindowMetadataDoc> setOffsetAndStreamTime(
      final int kafkaPartition,
      final long offset
  ) {
    final var segments = kafkaPartitionToSegments.get(kafkaPartition);
    final long epoch = segments.epoch;
    final long streamTime = segments.segmentBatch.batchStreamTime;

    LOG.info("{}[{}] Updating offset to {} and streamTime to {} with epoch {}",
             name, kafkaPartition, offset, streamTime, epoch);

    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowMetadataDoc.ID, kafkaPartition),
            Filters.lte(WindowMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowMetadataDoc.OFFSET, offset),
            Updates.set(WindowMetadataDoc.STREAM_TIME, streamTime),
            Updates.set(WindowMetadataDoc.EPOCH, epoch)
        )
    );
  }

  public long epoch(final int kafkaPartition) {
    return kafkaPartitionToSegments.get(kafkaPartition).epoch;
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
    partitionSegments.segmentBatch.updateStreamTime(windowedKey.windowStartMs);

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
    partitionSegments.segmentBatch.updateStreamTime(windowedKey.windowStartMs);
    throw new UnsupportedOperationException("Deletes not yet supported for MongoDB window stores");
  }

  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long windowStart
  ) {
    final var segment = partitioner.tablePartition(
        kafkaPartition,
        new WindowedKey(key, windowStart)
    );
    final var segmentWindows = windowsForSegmentPartition(kafkaPartition, segment);
    if (segmentWindows == null) {
      return null;
    }

    final WindowDoc windowDoc = segmentWindows.find(
        Filters.and(
            Filters.eq(WindowDoc.ID, compositeKey(key.get(), windowStart))))
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

    for (final var segment : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final var segmentWindows = partitionSegments.segmentWindows.get(segment);
      final FindIterable<WindowDoc> fetchResults = segmentWindows.find(
          Filters.and(
              Filters.gte(WindowDoc.ID, compositeKey(key.get(), timeFrom)),
              Filters.lte(WindowDoc.ID, compositeKey(key.get(), timeTo))));

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

  private static KeyValue<WindowedKey, byte[]> windowFromDoc(final WindowDoc windowDoc) {
    return new KeyValue<>(WindowDoc.windowedKey(windowDoc.id), windowDoc.value);
  }

}

