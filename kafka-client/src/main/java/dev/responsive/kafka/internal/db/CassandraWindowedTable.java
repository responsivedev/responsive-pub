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

package dev.responsive.kafka.internal.db;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.DATA_VALUE;
import static dev.responsive.kafka.internal.db.ColumnName.EPOCH;
import static dev.responsive.kafka.internal.db.ColumnName.METADATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.METADATA_TS;
import static dev.responsive.kafka.internal.db.ColumnName.OFFSET;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.ROW_TYPE;
import static dev.responsive.kafka.internal.db.ColumnName.SEGMENT_ID;
import static dev.responsive.kafka.internal.db.ColumnName.WINDOW_START;
import static dev.responsive.kafka.internal.db.RowType.DATA_ROW;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;
import static java.util.Collections.singletonList;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentRoll;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Stamped;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWindowedTable implements
    RemoteWindowedTable<BoundStatement>, TableMetadata<SegmentPartition> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowedTable.class);

  private static final String KEY_FROM_BIND = "kf";
  private static final String KEY_TO_BIND = "kt";
  private static final String WINDOW_FROM_BIND = "wf";
  private static final String WINDOW_TO_BIND = "wt";

  private final String name;
  private final CassandraClient client;
  private final SegmentPartitioner partitioner;

  private final PreparedStatement initSegment;
  private final PreparedStatement expireSegment;
  private final PreparedStatement insert;
  private final PreparedStatement delete;
  private final PreparedStatement fetchSingle;
  private final PreparedStatement fetch;
  private final PreparedStatement fetchRange;
  private final PreparedStatement fetchAll;
  private final PreparedStatement backFetch;
  private final PreparedStatement backFetchRange;
  private final PreparedStatement backFetchAll;
  private final PreparedStatement fetchOffset;
  private final PreparedStatement setOffset;
  private final PreparedStatement fetchEpoch;
  private final PreparedStatement reserveEpoch;
  private final PreparedStatement ensureEpoch;

  // Note: we intentionally track stream-time separately here and in the state store itself
  // as these entities have different views of the current time and should not be unified.
  // (Specifically, this table will always lag the view of stream-time that is shared by the
  // ResponsiveWindowStore and CommitBuffer due to buffering/batching of writes)
  private long lastFlushStreamTime = 0L;
  private long pendingFlushStreamTime = 0L;

  public static CassandraWindowedTable create(
      final CassandraTableSpec spec,
      final CassandraClient client
  ) throws InterruptedException, TimeoutException {
    final String name = spec.tableName();

    // TODO(window): explore better data models for fetchRange/fetchAll
    // Cassandra does not support filtering on a composite key column if
    // the previous columns in the composite are not equality filters
    // in the table below, for example, we cannot filter on WINDOW_START
    // unless DATA_KEY is an equality filter, or vice versa -- because
    // of the way SSTables are used in Cassandra this would be inefficient
    //
    // Until we figure out a better data model we just filter on the
    // DATA_KEY and then post-filter the results to match the time bounds.
    // Although we may fetch results that don't strictly fall within the
    // query bounds, the extra data is limited to at most twice the
    // segment interval, since the segments already narrow down the time
    // range although at a more coarse-grained size
    //
    // This is probably sufficient for now, as the key range fetches --
    // especially the bounded key-range fetch, ie fetchRange -- are both
    // relatively quite uncommon in Streams applications. Note that the
    // DSL only uses point or single-key lookups, and even among PAPI
    // users, key-range queries are typically rare due to several factors
    // (mainly the unpredictable ordering, as well as unidentifiable
    // bounds for the fetchRange queries, etc)
    LOG.info("Creating windowed data table {} in remote store.", name);
    final CreateTableWithOptions createTable = spec.applyOptions(createTable(name));

    client.execute(createTable.build());
    client.awaitTable(name).await(Duration.ofSeconds(60));

    // TODO: consolidate the #init method into the constructor and create the
    //  LwtWriterFactory before preparing the statements so we can just plug
    //  in the epoch directly without having to bind it later/on each request
    final var initSegment = client.prepare(
        QueryBuilder.insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(SEGMENT_ID.column(), bindMarker(SEGMENT_ID.bind()))
            .value(ROW_TYPE.column(), METADATA_ROW.literal())
            .value(DATA_KEY.column(), DATA_KEY.literal(ColumnName.METADATA_KEY))
            .value(WINDOW_START.column(), WINDOW_START.literal(METADATA_TS))
            .value(EPOCH.column(), bindMarker(EPOCH.bind()))
            .ifNotExists()
            .build()
    );

    // TODO: explore how to guard against accidental resurrection of deleted segments by
    //  lagging StreamThreads that haven't yet realized they have been fenced.
    //  We may be able to do something more tricky once we preserve the stream-time in
    //  the metadata partition/row, by tracking all created segments and making sure to
    //  clean up any that are expired if/when we get fenced and have fetched the latest
    //  persisted stream-time. Alternatively, we can just leave around tombstone partitions
    // that are empty except for the metadata/epoch, rather than deleting them outright
    final var expireSegment = client.prepare(
        QueryBuilder.deleteFrom(name)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .build()
    );

    final var insert = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(SEGMENT_ID.column(), bindMarker(SEGMENT_ID.bind()))
            .value(ROW_TYPE.column(), DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(WINDOW_START.column(), bindMarker(WINDOW_START.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build()
    );

    final var delete = client.prepare(
        QueryBuilder
            .deleteFrom(name)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isEqualTo(bindMarker(WINDOW_START.bind())))
            .build()
    );

    final var fetchSingle = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isEqualTo(bindMarker(WINDOW_START.bind())))
            .build()
    );

    final var fetch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(WINDOW_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(WINDOW_TO_BIND)))
            .build()
    );

    final var fetchRange = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(KEY_FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(KEY_TO_BIND)))
            .build()
    );

    final var fetchAll = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .build()
    );

    final var backFetch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(WINDOW_START.relation().isGreaterThanOrEqualTo(bindMarker(WINDOW_FROM_BIND)))
            .where(WINDOW_START.relation().isLessThan(bindMarker(WINDOW_TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var backFetchRange = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isGreaterThan(bindMarker(KEY_FROM_BIND)))
            .where(DATA_KEY.relation().isLessThan(bindMarker(KEY_TO_BIND)))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var backFetchAll = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), WINDOW_START.column(), DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .orderBy(DATA_KEY.column(), ClusteringOrder.DESC)
            .orderBy(WINDOW_START.column(), ClusteringOrder.DESC)
            .build()
    );

    final var fetchOffset = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build()
    );

    final var setOffset = client.prepare(QueryBuilder
        .update(name)
        .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
        .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
        .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
        .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
        .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
        .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
        .build()
    );

    final var fetchEpoch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(EPOCH.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .build()
    );

    final var reserveEpoch = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .ifColumn(EPOCH.column()).isLessThan(bindMarker(EPOCH.bind()))
            .build()
    );

    final var ensureEpoch = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    );

    // TODO: consider how to refactor the spec-wrapping based table creation so we can
    //  directly pass in a partitioner of the expected type and don't have to cast
    final SegmentPartitioner partitioner = (SegmentPartitioner) spec.partitioner();
    return new CassandraWindowedTable(
        name,
        client,
        partitioner,
        initSegment,
        expireSegment,
        insert,
        delete,
        fetchSingle,
        fetch,
        fetchAll,
        fetchRange,
        backFetch,
        backFetchAll,
        backFetchRange,
        fetchOffset,
        setOffset,
        fetchEpoch,
        reserveEpoch,
        ensureEpoch
    );
  }

  private static CreateTableWithOptions createTable(final String tableName) {
    return SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withPartitionKey(SEGMENT_ID.column(), DataTypes.BIGINT)
        .withClusteringColumn(ROW_TYPE.column(), DataTypes.TINYINT)
        .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
        .withClusteringColumn(WINDOW_START.column(), DataTypes.TIMESTAMP)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
        .withColumn(OFFSET.column(), DataTypes.BIGINT)
        .withColumn(EPOCH.column(), DataTypes.BIGINT);
  }

  public CassandraWindowedTable(
      final String name,
      final CassandraClient client,
      final SegmentPartitioner partitioner,
      final PreparedStatement initSegment,
      final PreparedStatement expireSegment,
      final PreparedStatement insert,
      final PreparedStatement delete,
      final PreparedStatement fetchSingle,
      final PreparedStatement fetch,
      final PreparedStatement fetchAll,
      final PreparedStatement fetchRange,
      final PreparedStatement backFetch,
      final PreparedStatement backFetchAll,
      final PreparedStatement backFetchRange,
      final PreparedStatement fetchOffset,
      final PreparedStatement setOffset,
      final PreparedStatement fetchEpoch,
      final PreparedStatement reserveEpoch,
      final PreparedStatement ensureEpoch
  ) {
    this.name = name;
    this.client = client;
    this.partitioner = partitioner;
    this.initSegment = initSegment;
    this.expireSegment = expireSegment;
    this.insert = insert;
    this.delete = delete;
    this.fetchSingle = fetchSingle;
    this.fetch = fetch;
    this.fetchAll = fetchAll;
    this.fetchRange = fetchRange;
    this.backFetch = backFetch;
    this.backFetchAll = backFetchAll;
    this.backFetchRange = backFetchRange;
    this.fetchOffset = fetchOffset;
    this.setOffset = setOffset;
    this.fetchEpoch = fetchEpoch;
    this.reserveEpoch = reserveEpoch;
    this.ensureEpoch = ensureEpoch;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WriterFactory<Stamped<Bytes>, SegmentPartition> init(
      final int kafkaPartition
  ) {
    // TODO(sophie): store the stream-time in the offset metadata row and retrieve it to
    //  initialize the state store's tracked stream-time and reserve the epoch for all
    //  currently active segments
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);

    return LwtWriterFactory.initialize(
        this,
        this,
        client,
        partitioner,
        kafkaPartition,
        singletonList(metadataPartition)
    );
  }

  @Override
  public void advanceStreamTime(
      final int kafkaPartition,
      final long epoch
  ) {
    if (pendingFlushStreamTime > lastFlushStreamTime) {
      final SegmentRoll roll =
          partitioner.rolledSegments(lastFlushStreamTime, pendingFlushStreamTime);

      LOG.info("Advancing stream-time for table {} from {}ms to {}ms and rolling segments: {}",
               name, lastFlushStreamTime, pendingFlushStreamTime, roll);

      for (final long segmentId : roll.segmentsToExpire) {
        expireSegment(new SegmentPartition(kafkaPartition, segmentId));
      }

      for (final long segmentId : roll.segmentsToCreate) {
        initSegment(new SegmentPartition(kafkaPartition, segmentId), epoch);
      }

      lastFlushStreamTime = pendingFlushStreamTime;
    }
  }

  private void initSegment(final SegmentPartition segmentToCreate, final long epoch) {
    client.execute(
        initSegment
            .bind()
            .setInt(PARTITION_KEY.bind(), segmentToCreate.partitionKey)
            .setLong(SEGMENT_ID.bind(), segmentToCreate.segmentId)
            .setLong(EPOCH.bind(), epoch)
    );
  }

  private void expireSegment(final SegmentPartition segmentToDelete) {
    client.execute(
        expireSegment
            .bind()
            .setInt(PARTITION_KEY.bind(), segmentToDelete.partitionKey)
            .setLong(SEGMENT_ID.bind(), segmentToDelete.segmentId)
    );
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final List<Row> result = client.execute(
        fetchOffset
            .bind()
            .setInt(PARTITION_KEY.bind(), metadataPartition.partitionKey)
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
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), kafkaPartition)
        .setLong(OFFSET.bind(), offset);
  }

  @Override
  public long fetchEpoch(final SegmentPartition segmentPartition) {
    final List<Row> result = client.execute(
            fetchEpoch
                .bind()
                .setInt(PARTITION_KEY.bind(), segmentPartition.partitionKey)
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
        .setInt(PARTITION_KEY.bind(), segmentPartition.partitionKey)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setLong(EPOCH.bind(), epoch);
  }

  @Override
  public BoundStatement ensureEpoch(final SegmentPartition segmentPartition, final long epoch) {
    return ensureEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.partitionKey)
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
      final Stamped<Bytes> key,
      final byte[] value,
      final long epochMillis
  ) {
    pendingFlushStreamTime = Math.max(pendingFlushStreamTime, key.stamp);
    final SegmentPartition remotePartition = partitioner.tablePartition(kafkaPartition, key);
    return insert
        .bind()
        .setInt(PARTITION_KEY.bind(), remotePartition.partitionKey)
        .setLong(SEGMENT_ID.bind(), remotePartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp))
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
      final Stamped<Bytes> key
  ) {
    pendingFlushStreamTime = Math.max(pendingFlushStreamTime, key.stamp);
    final SegmentPartition segmentPartition = partitioner.tablePartition(kafkaPartition, key);
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.partitionKey)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.stamp));
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
    final Stamped<Bytes> windowedKey = new Stamped<>(key, windowStart);
    final SegmentPartition segmentPartition =
        partitioner.tablePartition(kafkaPartition, windowedKey);

    final BoundStatement get = fetchSingle
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.partitionKey)
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetch
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
          .setInstant(WINDOW_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(WINDOW_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.kv(
        Iterators.wrapped(segmentIterators),
        CassandraWindowedTable::windowRows
    );
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetch
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
          .setInstant(WINDOW_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(WINDOW_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.kv(
        Iterators.wrapped(segmentIterators),
        CassandraWindowedTable::windowRows
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetchRange
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(KEY_FROM_BIND, ByteBuffer.wrap(fromKey.get()))
          .setByteBuffer(KEY_TO_BIND, ByteBuffer.wrap(toKey.get()));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.filterKv(
        Iterators.kv(Iterators.wrapped(segmentIterators), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetchRange
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setByteBuffer(KEY_FROM_BIND, ByteBuffer.wrap(fromKey.get()))
          .setByteBuffer(KEY_TO_BIND, ByteBuffer.wrap(toKey.get()));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.filterKv(
        Iterators.kv(Iterators.wrapped(segmentIterators), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.range(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = fetchAll
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setInstant(KEY_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(KEY_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.filterKv(
        Iterators.kv(Iterators.wrapped(segmentIterators), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
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
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<Iterator<Row>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.reverseRange(kafkaPartition, timeFrom, timeTo)) {
      final BoundStatement get = backFetchAll
          .bind()
          .setInt(PARTITION_KEY.bind(), partition.partitionKey)
          .setLong(SEGMENT_ID.bind(), partition.segmentId)
          .setInstant(KEY_FROM_BIND, Instant.ofEpochMilli(timeFrom))
          .setInstant(KEY_TO_BIND, Instant.ofEpochMilli(timeTo));

      final ResultSet result = client.execute(get);
      segmentIterators.add(result.iterator());
    }

    return Iterators.filterKv(
        Iterators.kv(Iterators.wrapped(segmentIterators), CassandraWindowedTable::windowRows),
        k -> k.stamp >= timeFrom && k.stamp < timeTo
    );
  }

  private static KeyValue<Stamped<Bytes>, byte[]> windowRows(final Row row) {
    final long startTs = row.getInstant(WINDOW_START.column()).toEpochMilli();
    final Bytes key = Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array());

    return new KeyValue<>(
        new Stamped<>(key, startTs),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

}
