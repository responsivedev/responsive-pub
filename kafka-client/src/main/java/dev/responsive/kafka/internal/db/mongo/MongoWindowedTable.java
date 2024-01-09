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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_VALUE;
import static dev.responsive.kafka.internal.db.ColumnName.EPOCH;
import static dev.responsive.kafka.internal.db.ColumnName.OFFSET;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.SEGMENT_ID;
import static dev.responsive.kafka.internal.db.ColumnName.STREAM_TIME;
import static dev.responsive.kafka.internal.db.ColumnName.WINDOW_START;
import static dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.UNINITIALIZED_STREAM_TIME;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
import dev.responsive.kafka.internal.db.CassandraWindowedTable;
import dev.responsive.kafka.internal.db.LwtWriterFactory;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentRoll;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.bson.Document;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWindowedTable implements RemoteWindowedTable<WriteModel<Document>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoWindowedTable.class);
  private static final String METADATA_COLLECTION_SUFFIX = "_md";

  private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

  private final String name;
  private final SegmentPartitioner partitioner;

  private final MongoCollection<WindowDoc> windows;
  private final MongoCollection<WindowMetadataDoc> metadata;

  private final MongoCollection<Document> genericWindows;
  private final MongoCollection<Document> genericMetadata;

  // Note: we intentionally track stream-time separately here and in the state store itself
  // as these entities have different views of the current time and should not be unified.
  // (Specifically, this table will always lag the view of stream-time that is shared by the
  // ResponsiveWindowStore and CommitBuffer due to buffering/batching of writes)
  private final Map<Integer, PendingFlushInfo> kafkaPartitionToPendingFlushInfo =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Long> kafkaPartitionToEpoch = new ConcurrentHashMap<>();

  // Recommended to keep the total number of collections (and thus segments) under 10,000
  private final ConcurrentMap<SegmentPartition, >

  // TODO: move this into the Writer/Factory or new class to keep the table stateless
  private static class PendingFlushInfo {

    private long lastFlushStreamTime;
    private long pendingFlushStreamTime;
    private SegmentRoll segmentRoll;

    public PendingFlushInfo(final long persistedStreamTime) {
      this.lastFlushStreamTime = persistedStreamTime;
      this.pendingFlushStreamTime = persistedStreamTime;
    }

    void maybeUpdatePendingStreamTime(final long recordTimestamp) {
      this.pendingFlushStreamTime = Math.max(pendingFlushStreamTime, recordTimestamp);
    }

    void initSegmentRoll(final SegmentRoll pendingSegmentRoll) {
      this.segmentRoll = pendingSegmentRoll;
    }

    void finalizeFlush() {
      segmentRoll = null;
      lastFlushStreamTime = pendingFlushStreamTime;
    }
  }

  public MongoWindowedTable(
      final MongoClient client,
      final String name,
      final SegmentPartitioner partitioner
  ) {
    this.name = name;
    this.partitioner = partitioner;

    CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
    CodecRegistry pojoCodecRegistry = fromRegistries(
        getDefaultCodecRegistry(),
        fromProviders(pojoCodecProvider)
    );

    final MongoDatabase database = client.getDatabase(name).withCodecRegistry(pojoCodecRegistry);
    genericWindows = database.getCollection(name);
    windows = database.getCollection(name, WindowDoc.class);

    genericMetadata = database.getCollection(name + METADATA_COLLECTION_SUFFIX);
    metadata = database.getCollection(name + METADATA_COLLECTION_SUFFIX, WindowMetadataDoc.class);

    windows.createIndex(
        Indexes.ascending(WindowDoc.WINDOW_START_TS)
    );
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<WindowedKey, SegmentPartition> init(
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

    LOG.info("Retrieved initial metadata {}", metaDoc);

    kafkaPartitionToEpoch.put(kafkaPartition, metaDoc.epoch);
    kafkaPartitionToPendingFlushInfo.put(kafkaPartition, new PendingFlushInfo(metaDoc.streamTime));

    // TODO - remove below
    final var activeSegments = partitioner.activeSegments(kafkaPartition, streamTime);
    if (activeSegments.isEmpty()) {
      LOG.info("Skipping reservation of epoch {} for kafka partition {} due to no active segments",
               epoch, kafkaPartition);
    } else {
      final long firstSegmentId = activeSegments.get(0).segmentId;
      long lastSegmentId = firstSegmentId;
      for (final SegmentPartition tablePartition : activeSegments) {
        final var reserveSegmentEpoch = client.execute(reserveEpoch(tablePartition, epoch));

        if (!reserveSegmentEpoch.wasApplied()) {
          handleEpochFencing(kafkaPartition, tablePartition, epoch);
        }
        lastSegmentId = tablePartition.segmentId;
      }
      LOG.info("Reserved epoch {} for kafka partition {} across active segments in range {} - {}",
               epoch, kafkaPartition, firstSegmentId, lastSegmentId);
    }
    
    // TODO - remove above

    return new MongoWriterFactory<>(this, genericWindows, genericMetadata, kafkaPartition);
  }

  @Override
  public void preCommit(
      final int kafkaPartition,
      final long epoch
  ) {
    final PendingFlushInfo pendingFlush = kafkaPartitionToPendingFlushInfo.get(kafkaPartition);
    final SegmentRoll pendingRoll = partitioner.rolledSegments(
        name, pendingFlush.lastFlushStreamTime, pendingFlush.pendingFlushStreamTime
    );

    pendingFlush.initSegmentRoll(pendingRoll);
    for (final long segmentId : pendingRoll.segmentsToCreate) {
      final SegmentPartition segment = new SegmentPartition(kafkaPartition, segmentId);
      final var createSegment = client.execute(createSegment(segment, epoch));

      // If the segment creation failed because the table partition already exists, attempt to
      // update the epoch in case we are fencing an older writer -- if that fails it means we're
      // the ones being fenced
      if (!createSegment.wasApplied()) {
        final var reserveEpoch = client.execute(reserveEpoch(segment, epoch));

        if (!reserveEpoch.wasApplied()) {
          handleEpochFencing(kafkaPartition, segment, epoch);
        }
      }
    }
  }

  @Override
  public void postCommit(
      final int kafkaPartition,
      final long epoch
  ) {
    final PendingFlushInfo pendingFlush = kafkaPartitionToPendingFlushInfo.get(kafkaPartition);
    for (final long segmentId : pendingFlush.segmentRoll.segmentsToExpire) {
      // TODO: check result of expiration
      client.execute(expireSegment(new SegmentPartition(kafkaPartition, segmentId)));
    }
    pendingFlush.finalizeFlush();
  }

  private BoundStatement createSegment(final SegmentPartition segmentToCreate, final long epoch) {
    return createSegment
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentToCreate.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentToCreate.segmentId)
        .setLong(EPOCH.bind(), epoch)
        .setIdempotent(true);
  }

  private BoundStatement expireSegment(final SegmentPartition segmentToDelete) {
    return expireSegment
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentToDelete.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentToDelete.segmentId)
        .setIdempotent(true);
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

  @Override
  public WriteModel<Document> setOffset(
      final int kafkaPartition,
      final long offset
  ) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowMetadataDoc.ID, kafkaPartition),
            Filters.lte(WindowMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowMetadataDoc.OFFSET, offset),
            Updates.set(WindowMetadataDoc.EPOCH, epoch)
        )
    );
  }

  // TODO(sophie): combine with setOffset
  public WriteModel<Document> setStreamTime(
      final int kafkaPartition,
      final long epoch
  ) {
    final PendingFlushInfo pendingFlush = kafkaPartitionToPendingFlushInfo.get(kafkaPartition);

    LOG.debug("Updating stream-time to {} with epoch {} for kafkaPartition {}",
              pendingFlush.pendingFlushStreamTime, epoch, kafkaPartition);

    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowMetadataDoc.ID, kafkaPartition),
            Filters.lte(WindowMetadataDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowMetadataDoc.STREAM_TIME, pendingFlush.pendingFlushStreamTime),
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
  public WriteModel<Document> insert(
      final int kafkaPartition,
      final WindowedKey windowedKey,
      final byte[] value,
      final long epochMillis
  ) {
    // TODO(sophie): implement segments via Collections
    final SegmentPartition segmentPartition = partitioner.tablePartition(kafkaPartition, windowedKey);

    kafkaPartitionToPendingFlushInfo.get(kafkaPartition)
        .maybeUpdatePendingStreamTime(windowedKey.windowStartMs);

    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowDoc.ID, windowedKey.key.get()),
            Filters.eq(WindowDoc.WINDOW_START_TS, windowedKey.windowStartMs),
            Filters.lte(WindowDoc.EPOCH, epoch)
        ),
        Updates.combine(
            Updates.set(WindowDoc.VALUE, value),
            Updates.set(WindowDoc.EPOCH, epoch),
            Updates.unset(WindowDoc.TOMBSTONE_TS) // TODO(sophie) -- how do we want to handle this?
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
  public WriteModel<Document> delete(
      final int kafkaPartition,
      final WindowedKey windowedKey
  ) {
    throw new UnsupportedOperationException("Deletes not yet supported for MongoDB window stores");
  }

  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long windowStart
  ) {
    final WindowDoc windowDoc = windows.find(
        Filters.and(
            Filters.eq(WindowDoc.ID, key.get()),
            Filters.eq(WindowDoc.WINDOW_START_TS, windowStart)))
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
    final FindIterable<WindowDoc> fetchResults = windows.find(
            Filters.and(
                Filters.eq(WindowDoc.ID, key.get()),
                Filters.gte(WindowDoc.WINDOW_START_TS, timeFrom),
                Filters.lte(WindowDoc.WINDOW_START_TS, timeTo)));

    return Iterators.kv(
        fetchResults.iterator(),
        doc -> new KeyValue<>(new WindowedKey(doc.id, doc.windowStartTs), doc.value)
    );
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
    final FindIterable<WindowDoc> fetchResults = windows.find(
        Filters.and(
            Filters.gte(WindowDoc.ID, fromKey.get()),
            Filters.lte(WindowDoc.ID, toKey.get()),
            Filters.gte(WindowDoc.WINDOW_START_TS, timeFrom),
            Filters.lte(WindowDoc.WINDOW_START_TS, timeTo)));

    return Iterators.kv(
        fetchResults.iterator(),
        doc -> new KeyValue<>(new WindowedKey(doc.id, doc.windowStartTs), doc.value)
    );
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("backFetchRange not yet supported for MongoDB backends");
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final FindIterable<WindowDoc> fetchResults = windows.find(
        Filters.and(
            Filters.gte(WindowDoc.WINDOW_START_TS, timeFrom),
            Filters.lte(WindowDoc.WINDOW_START_TS, timeTo)));

    return Iterators.kv(
        fetchResults.iterator(),
        doc -> new KeyValue<>(new WindowedKey(doc.id, doc.windowStartTs), doc.value)
    );
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("backFetchAll not yet supported for MongoDB backends");
  }

}

