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
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
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

  private final String name;
  private final SegmentPartitioner partitioner;

  private final MongoCollection<WindowDoc> windows;
  private final MongoCollection<WindowMetadataDoc> metadata;

  private final MongoCollection<Document> genericWindows;
  private final MongoCollection<Document> genericMetadata;

  // this map contains the initialized value of the metadata document,
  // for which the epoch and object ID will never change. it is likely
  // that after a few writes to MongoDB the cached MetadataDoc.offset value
  // will be out of date, so it should not be used beyond initialization
  private final ConcurrentMap<Integer, MetadataDoc> metadataRows = new ConcurrentHashMap<>();

  // Note: we intentionally track stream-time separately here and in the state store itself
  // as these entities have different views of the current time and should not be unified.
  // (Specifically, this table will always lag the view of stream-time that is shared by the
  // ResponsiveWindowStore and CommitBuffer due to buffering/batching of writes)
  private final Map<Integer, PendingFlushInfo> kafkaPartitionToPendingFlushInfo = new HashMap<>();

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
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
/*
    final var initMetadata = client.execute(
        QueryBuilder.insertInto(name)
            .value(PARTITION_KEY.column(), PARTITION_KEY.literal(metadataPartition.tablePartition))
            .value(SEGMENT_ID.column(), SEGMENT_ID.literal(metadataPartition.segmentId))
            .value(ROW_TYPE.column(), METADATA_ROW.literal())
            .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
            .value(WINDOW_START.column(), WINDOW_START.literal(METADATA_TS))
            .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
            .value(EPOCH.column(), EPOCH.literal(0L))
            .value(STREAM_TIME.column(), STREAM_TIME.literal(UNINITIALIZED_STREAM_TIME))
            .ifNotExists()
            .build()
    );



    if (initMetadata.wasApplied()) {
      LOG.info("Created new metadata segment for kafka partition {}", kafkaPartition);
    }

 */

    final long epoch = fetchEpoch(metadataPartition) + 1;
    final var reserveMetadataEpoch = client.execute(reserveEpoch(metadataPartition, epoch));
    if (!reserveMetadataEpoch.wasApplied()) {
      handleEpochFencing(kafkaPartition, metadataPartition, epoch);
    }

    final long streamTime = fetchStreamTime(kafkaPartition);
    kafkaPartitionToPendingFlushInfo.put(kafkaPartition, new PendingFlushInfo(streamTime));
    LOG.info("Initialized stream-time to {} with epoch {} for kafka partition {}",
             streamTime, epoch, kafkaPartition);

    // since the active data segments depend on the current stream-time for the windowed table,
    // which we won't know until we initialize it from the remote, the metadata like epoch and
    // stream-time are stored in a special metadata partition/segment that's separate from the
    // regular data partitions/segments and never expired
    // therefore we initialize from the metadata partition and then broadcast the epoch to
    // all the other partitions containing data for active segments
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

    return new LwtWriterFactory<>(
        this,
        this,
        client,
        partitioner,
        kafkaPartition,
        epoch
    );
  }

  // TODO: check whether we need to throw a CommitFailedException or ProducerFencedException
  //  instead, or whether TaskMigratedException thrown here will be properly handled by Streams
  private void handleEpochFencing(
      final int kafkaPartition,
      final SegmentPartition tablePartition,
      final long epoch
  ) {
    final long otherEpoch = fetchEpoch(tablePartition);
    final var msg = String.format(
        "Could not initialize commit buffer [%s-%d] - attempted to claim epoch %d, "
            + "but was fenced by a writer that claimed epoch %d on table partition %s",
        name(),
        kafkaPartition,
        epoch,
        otherEpoch,
        tablePartition
    );
    final var e = new TaskMigratedException(msg);
    LOG.warn(msg, e);
    throw e;
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
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final List<Row> result = client.execute(
            fetchOffset
                .bind()
                .setInt(PARTITION_KEY.bind(), metadataPartition.tablePartition)
                .setLong(SEGMENT_ID.bind(), metadataPartition.segmentId))
        .all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          name, kafkaPartition, result.size()));
    } else {
      return result.get(0).getLong(OFFSET.column());
    }
  }

  @Override
  public BoundStatement setOffset(
      final int kafkaPartition,
      final long offset
  ) {
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), metadataPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), metadataPartition.segmentId)
        .setLong(OFFSET.bind(), offset);
  }

  public long fetchStreamTime(final int kafkaPartition) {
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final List<Row> result = client.execute(
            fetchStreamTime
                .bind()
                .setInt(PARTITION_KEY.bind(), metadataPartition.tablePartition)
                .setLong(SEGMENT_ID.bind(), metadataPartition.segmentId))
        .all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one stream-time row for %s[%s] but got %d",
          name, kafkaPartition, result.size()));
    } else {
      return result.get(0).getLong(STREAM_TIME.column());
    }
  }

  public BoundStatement setStreamTime(
      final int kafkaPartition,
      final long epoch
  ) {
    final PendingFlushInfo pendingFlush = kafkaPartitionToPendingFlushInfo.get(kafkaPartition);

    LOG.debug("Updating stream-time to {} with epoch {} for kafkaPartition {}",
              pendingFlush.pendingFlushStreamTime, epoch, kafkaPartition);

    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    return setStreamTime
        .bind()
        .setInt(PARTITION_KEY.bind(), metadataPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), metadataPartition.segmentId)
        .setLong(STREAM_TIME.bind(), pendingFlush.pendingFlushStreamTime);
  }

  @Override
  public long fetchEpoch(final SegmentPartition segmentPartition) {
    final List<Row> result = client.execute(
            fetchEpoch
                .bind()
                .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
                .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId))
        .all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one epoch metadata row for %s[%s] but got %d",
          name, segmentPartition, result.size()));
    } else {
      return result.get(0).getLong(EPOCH.column());
    }
  }

  @Override
  public BoundStatement reserveEpoch(final SegmentPartition segmentPartition, final long epoch) {
    return reserveEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setLong(EPOCH.bind(), epoch);
  }

  @Override
  public BoundStatement ensureEpoch(final SegmentPartition segmentPartition, final long epoch) {
    return ensureEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setLong(EPOCH.bind(), epoch);
  }

  /**
   * Inserts data into {@code table}. Note that this will overwrite
   * any existing entry in the table with the same key.
   *
   * @param kafkaPartition the kafka partition
   * @param key            the data key
   * @param value          the data value
   * @param epochMillis    the timestamp of the event
   * @return a statement that, when executed, will insert the row
   *         matching {@code partitionKey} and {@code key} in the
   *         {@code table} with value {@code value}. Note that the
   *         {@code key} here is the "windowed key" which includes
   *         both the record key and also the windowStart timestamp
   */
  @Override
  @CheckReturnValue
  public BoundStatement insert(
      final int kafkaPartition,
      final WindowedKey key,
      final byte[] value,
      final long epochMillis
  ) {
    maybeUpdateStreamTime(kafkaPartition, key.windowStartMs);

    final SegmentPartition remotePartition = partitioner.tablePartition(kafkaPartition, key);
    return insert
        .bind()
        .setInt(PARTITION_KEY.bind(), remotePartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), remotePartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.windowStartMs))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  /**
   * @param kafkaPartition  the kafka partition
   * @param key             the data key
   *
   * @return a statement that, when executed, will delete the row
   *         matching {@code kafkaPartition} and {@code key} in the
   *         {@code table}. Note that the {@code key} here is the
   *         "windowed key" which includes both the record key and
   *         also the window start timestamp
   */
  @Override
  @CheckReturnValue
  public BoundStatement delete(
      final int kafkaPartition,
      final WindowedKey key
  ) {
    maybeUpdateStreamTime(kafkaPartition, key.windowStartMs);

    final SegmentPartition segmentPartition = partitioner.tablePartition(kafkaPartition, key);
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.windowStartMs));
  }

  /**
   * Retrieves the value of the given {@code partitionKey} and {@code key} from {@code table}.
   *
   * @param kafkaPartition  the partition
   * @param key             the data key
   * @param windowStart     the start time of the window
   * @return the value previously set
   */
  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long windowStart
  ) {
    final WindowedKey windowedKey = new WindowedKey(key, windowStart);
    final SegmentPartition segmentPartition =
        partitioner.tablePartition(kafkaPartition, windowedKey);

    final BoundStatement get = fetchSingle
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(windowStart));

    final List<Row> result = client.execute(get).all();
    if (result.size() > 1) {
      throw new IllegalStateException("Unexpected multiple results for point lookup");
    } else if (result.isEmpty()) {
      return null;
    } else {
      final ByteBuffer value = result.get(0).getByteBuffer(DATA_VALUE.column());
      return Objects.requireNonNull(value).array();
    }
  }

  /**
   * Retrieves the range of windows of the given {@code partitionKey} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo} from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param key            the data key
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   * @return the windows previously stored
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetch
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
          .setInstant(WINDOW_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(WINDOW_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.wrapped(segmentIterators);
  }

  /**
   * Retrieves the range of window of the given {@code partitionKey} and {@code key} with a
   * start time between {@code timeFrom} and {@code timeTo} from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param key            the data key
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetch
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
          .setInstant(WINDOW_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(WINDOW_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.wrapped(segmentIterators);
  }

  /**
   * Retrieves the range of windows of the given {@code partitionKey} for all keys between
   * {@code fromKey} and {@code toKey} with a start time between {@code timeFrom} and {@code timeTo}
   * from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param fromKey        the min data key (inclusive)
   * @param toKey          the max data key (exclusive)
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetchRange
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(KEY_FROM_BIND, ByteBuffer.wrap(fromKey.get()))
          .setByteBuffer(KEY_TO_BIND, ByteBuffer.wrap(toKey.get()));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.filterKv(
        Iterators.wrapped(segmentIterators),
        k -> k.windowStartMs >= timeFrom && k.windowStartMs < timeTo
    );
  }

  /**
   * Retrieves the range of windows of the given {@code partitionKey} for all keys between
   * {@code fromKey} and {@code toKey} with a start time between {@code timeFrom} and {@code timeTo}
   * from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param fromKey        the min data key (inclusive)
   * @param toKey          the max data key (exclusive)
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetchRange
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(KEY_FROM_BIND, ByteBuffer.wrap(fromKey.get()))
          .setByteBuffer(KEY_TO_BIND, ByteBuffer.wrap(toKey.get()));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.filterKv(
        Iterators.wrapped(segmentIterators),
        k -> k.windowStartMs >= timeFrom && k.windowStartMs < timeTo
    );
  }

  /**
   * Retrieves the windows of the given {@code partitionKey} across all keys and times
   * from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetchAll
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setInstant(KEY_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(KEY_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.filterKv(
        Iterators.wrapped(segmentIterators),
        k -> k.windowStartMs >= timeFrom && k.windowStartMs < timeTo
    );
  }

  /**
   * Retrieves the windows of the given {@code partitionKey} across all keys and times
   * from {@code table}.
   *
   * @param kafkaPartition the partition
   * @param timeFrom       the min timestamp (inclusive)
   * @param timeTo         the max timestamp (exclusive)
   *
   * @return the value previously set
   */
  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetchAll
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.tablePartition)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setInstant(KEY_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(KEY_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(Iterators.kv(result.iterator(), CassandraWindowedTable::windowRows));
    }

    return Iterators.filterKv(
        Iterators.wrapped(segmentIterators),
        k -> k.windowStartMs >= timeFrom && k.windowStartMs < timeTo
    );
  }

  private void maybeUpdateStreamTime(final int kafkaPartition, final long timestamp) {
    kafkaPartitionToPendingFlushInfo.get(kafkaPartition).maybeUpdatePendingStreamTime(timestamp);
  }

  private static KeyValue<WindowedKey, byte[]> windowRows(final Row row) {
    final long startTs = row.getInstant(WINDOW_START.column()).toEpochMilli();
    final Bytes key = Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array());

    return new KeyValue<>(
        new WindowedKey(key, startTs),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

}

