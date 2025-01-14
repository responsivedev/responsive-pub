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
import static dev.responsive.kafka.internal.db.mongo.WindowDoc.ID_SUBFIELD_KEY;
import static dev.responsive.kafka.internal.db.mongo.WindowDoc.ID_SUBFIELD_WINDOW_START;
import static dev.responsive.kafka.internal.db.partitioning.Segmenter.UNINITIALIZED_STREAM_TIME;
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
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.MongoWindowFlushManager;
import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.partitioning.Segmenter;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.DuplicateKeyListValueIterator;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.ArrayList;
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
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWindowTable implements RemoteWindowTable<WriteModel<WindowDoc>> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoWindowTable.class);
  private static final String METADATA_COLLECTION_NAME = "window_metadata";

  private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);

  private final String name;
  private final WindowSegmentPartitioner partitioner;

  // whether to put windowStartMs first in the composite windowed key format in WindowDoc
  private final boolean timestampFirstOrder;
  // whether to save multiple records for each key or overwrite the existing value on insert
  private final boolean retainDuplicates;

  private final MongoDatabase database;
  private final MongoDatabase adminDatabase;
  private final MongoCollection<WindowMetadataDoc> metadata;
  private final ConcurrentMap<Integer, PartitionSegments> kafkaPartitionToSegments =
      new ConcurrentHashMap<>();
  private final CollectionCreationOptions collectionCreationOptions;
  private final KeyCodec keyCodec = new StringKeyCodec();

  private static class PartitionSegments {
    private final MongoDatabase database;
    private final MongoDatabase adminDatabase;
    private final Segmenter segmenter;
    private final long epoch;
    private final CollectionCreationOptions collectionCreationOptions;
    private final boolean timestampFirstOrder;

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
        final CollectionCreationOptions collectionCreationOptions,
        final boolean timestampFirstOrder
    ) {
      this.database = database;
      this.adminDatabase = adminDatabase;
      this.segmenter = segmenter;
      this.epoch = epoch;
      this.collectionCreationOptions = collectionCreationOptions;
      this.timestampFirstOrder = timestampFirstOrder;
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

    private String collectionNameForSegment(final SegmentPartition segmentPartition) {
      final long segmentStartTimeMs =
          segmentPartition.segmentStartTimestamp * segmenter.segmentIntervalMs();
      return String.format(
          "%d-%d",
          segmentPartition.tablePartition, segmentStartTimeMs
      );
    }

    private void createSegment(final SegmentPartition segmentToCreate) {
      LOG.info(
          "{}[{}] Creating segment start timestamp {}",
          database.getName(), segmentToCreate.tablePartition, segmentToCreate.segmentStartTimestamp
      );

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
      if (timestampFirstOrder) {
        windowDocs.createIndex(Indexes.compoundIndex(
            Indexes.ascending(WindowDoc.ID_WINDOW_START_TS),
            Indexes.ascending(WindowDoc.ID_RECORD_KEY)
        ));
      } else {
        windowDocs.createIndex(Indexes.compoundIndex(
            Indexes.ascending(WindowDoc.ID_RECORD_KEY),
            Indexes.ascending(WindowDoc.ID_WINDOW_START_TS)
        ));
      }
      segmentWindows.put(segmentToCreate, windowDocs);
    }

    // Note: it is always safe to drop a segment, even without validating the epoch, since we only
    // ever attempt to expire segments after successfully flushing the batch which rolled them,
    // thus guaranteeing that even if we are fenced, the new writer will pick up from an offset and
    // stream-time such that the segment is already expired
    // However this does leave us vulnerable to zombie segments. We should consider doing a sweep
    // for old segments every so often with #listCollectionNames, since the segmentStartTimestamp is
    // embedded in the collection name and will indicate whether it's part of the active set or
    // not, perhaps using a scheduled executor to avoid blocking processing.
    // In fact we may want to move all the segment expiration to a background process since there's
    // no async way to drop a collection but the stream thread doesn't actually need to wait for it
    private void deleteSegment(final SegmentPartition segmentToExpire) {
      LOG.info(
          "{}[{}] Expiring segment start timestamp {}",
          database.getName(), segmentToExpire.tablePartition, segmentToExpire.segmentStartTimestamp
      );

      final var expiredDocs = segmentWindows.get(segmentToExpire);
      expiredDocs.drop();
    }
  }

  public MongoWindowTable(
      final MongoClient client,
      final String name,
      final WindowSegmentPartitioner partitioner,
      final boolean timestampFirstOrder,
      final CollectionCreationOptions collectionCreationOptions
  ) {
    this.name = name;
    this.partitioner = partitioner;
    this.timestampFirstOrder = timestampFirstOrder;
    this.retainDuplicates = partitioner.retainDuplicates();
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

  @VisibleForTesting
  boolean isTimestampFirstOrder() {
    return timestampFirstOrder;
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
            collectionCreationOptions,
            timestampFirstOrder
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

  @VisibleForTesting
  MongoCollection<WindowDoc> windowsForSegmentPartition(
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
   * @param timestampMs    the timestamp of the event
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
      final long timestampMs
  ) {
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);
    final long epoch = partitionSegments.epoch;

    final var valueOperation = retainDuplicates
        ? Updates.push(WindowDoc.VALUES, value)
        : Updates.set(WindowDoc.VALUE, value);

    return new UpdateOneModel<>(
        Filters.and(
            Filters.eq(WindowDoc.ID, compositeKey(windowedKey)),
            Filters.lte(WindowDoc.EPOCH, epoch)
        ),
        Updates.combine(
            valueOperation,
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
    if (retainDuplicates) {
      throw new IllegalStateException("Should never attempt to issue a delete with duplicates");
    }

    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    // TODO: implement me!!
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

    if (windowDoc == null) {
      return null;
    } else if (retainDuplicates) {
      return windowDoc.getValues().get(0);
    } else {
      return windowDoc.getValue();
    }
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
      if (segmentWindows == null) {
        continue;
      }
      final ArrayList<Bson> filters = new ArrayList<>(timestampFirstOrder ? 3 : 2);
      // express this filter using the whole key rather than using separate expressions
      // for the record key and timestamp. This is because the _id index is not a composite
      // index, so if we don't use the whole key in the filter mongo will use a full table
      // scan. We can optimize this later by adding a composite index on timestamp, key
      // if using timestamp first order to hopefully avoid scanning all keys in the timestamp
      // range.
      filters.add(Filters.gte(WindowDoc.ID, compositeKey(key, timeFrom)));
      filters.add(Filters.lte(WindowDoc.ID, compositeKey(key, timeTo)));
      if (timestampFirstOrder) {
        // if we use timestamp-first order, then we need to check equality on the key,
        // because other keys will be interleaved in the mongodb range scan
        filters.add(
            Filters.eq(
                String.join(".", WindowDoc.ID, WindowDoc.ID_RECORD_KEY),
                keyCodec.encode(key)
            )
        );
      }
      final FindIterable<WindowDoc> fetchResults = segmentWindows.find(Filters.and(filters));

      if (retainDuplicates) {
        segmentIterators.add(
            new DuplicateKeyListValueIterator(fetchResults.iterator(), this::windowedKeyFromDoc));
      } else {
        segmentIterators.add(Iterators.kv(fetchResults.iterator(), this::windowFromDoc));
      }
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
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    for (final var segment : partitioner.segmenter().range(kafkaPartition, timeFrom, timeTo)) {
      final var segmentWindows = partitionSegments.segmentWindows.get(segment);
      if (segmentWindows == null) {
        continue;
      }
      final ArrayList<Bson> filters = new ArrayList<>(4);

      // order matters when filtering on fields in a compound index
      if (timestampFirstOrder) {
        filters.add(Filters.gte(ID_SUBFIELD_WINDOW_START, timeFrom));
        filters.add(Filters.lte(ID_SUBFIELD_WINDOW_START, timeTo));
        filters.add(Filters.gte(ID_SUBFIELD_KEY, keyCodec.encode(fromKey)));
        filters.add(Filters.lte(ID_SUBFIELD_KEY, keyCodec.encode(toKey)));
      } else {
        filters.add(Filters.gte(ID_SUBFIELD_KEY, keyCodec.encode(fromKey)));
        filters.add(Filters.lte(ID_SUBFIELD_KEY, keyCodec.encode(toKey)));
        filters.add(Filters.gte(ID_SUBFIELD_WINDOW_START, timeFrom));
        filters.add(Filters.lte(ID_SUBFIELD_WINDOW_START, timeTo));
      }
      final FindIterable<WindowDoc> fetchResults = segmentWindows.find(Filters.and(filters));

      if (retainDuplicates) {
        segmentIterators.add(
            new DuplicateKeyListValueIterator(fetchResults.iterator(), this::windowedKeyFromDoc));
      } else {
        segmentIterators.add(Iterators.kv(fetchResults.iterator(), this::windowFromDoc));
      }
    }
    return Iterators.wrapped(segmentIterators);
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
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    final var partitionSegments = kafkaPartitionToSegments.get(kafkaPartition);

    for (final var segment : partitioner.segmenter().range(kafkaPartition, timeFrom, timeTo)) {
      final var segmentWindows = partitionSegments.segmentWindows.get(segment);
      if (segmentWindows == null) {
        continue;
      }
      final ArrayList<Bson> filters = new ArrayList<>(2);
      filters.add(Filters.gte(ID_SUBFIELD_WINDOW_START, timeFrom));
      filters.add(Filters.lte(ID_SUBFIELD_WINDOW_START, timeTo));

      if (!timestampFirstOrder) {
        LOG.warn("WindowStore#fetchAll should be used with caution as a full table scan is "
                     + "required for range queries with no key bound when the key is indexed "
                     + "first. If your application makes heavy use of this API, consider "
                     + "setting " + ResponsiveConfig.MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_CONFIG
                     + "to 'true' for better performance of #fetchAll at the cost of worse "
                     + "performance of the #fetch(key, timeFrom, timeTo API.");
      }

      final FindIterable<WindowDoc> fetchResults = segmentWindows.find(Filters.and(filters));

      if (retainDuplicates) {
        segmentIterators.add(
            new DuplicateKeyListValueIterator(fetchResults.iterator(), this::windowedKeyFromDoc));
      } else {
        segmentIterators.add(Iterators.kv(fetchResults.iterator(), this::windowFromDoc));
      }
    }
    return Iterators.wrapped(segmentIterators);
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
        keyCodec.encode(key),
        windowStartMs,
        timestampFirstOrder
    );
  }

  private WindowedKey windowedKeyFromDoc(final WindowDoc windowDoc) {
    return new WindowedKey(
        keyCodec.decode(windowDoc.unwrapRecordKey()),
        windowDoc.unwrapWindowStartTs()
    );
  }

  private KeyValue<WindowedKey, byte[]> windowFromDoc(final WindowDoc windowDoc) {
    return new KeyValue<>(
        windowedKeyFromDoc(windowDoc),
        windowDoc.value
    );
  }

}

