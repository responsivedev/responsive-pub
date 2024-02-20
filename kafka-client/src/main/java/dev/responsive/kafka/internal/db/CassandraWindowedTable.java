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
import static dev.responsive.kafka.internal.db.ColumnName.STREAM_TIME;
import static dev.responsive.kafka.internal.db.ColumnName.WINDOW_START;
import static dev.responsive.kafka.internal.db.RowType.DATA_ROW;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;
import static dev.responsive.kafka.internal.db.partitioning.Segmenter.UNINITIALIZED_STREAM_TIME;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.internal.querybuilder.schema.compaction.DefaultLeveledCompactionStrategy;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import javax.annotation.CheckReturnValue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWindowedTable implements RemoteWindowedTable<BoundStatement> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraWindowedTable.class);

  private static final String KEY_FROM_BIND = "kf";
  private static final String KEY_TO_BIND = "kt";
  private static final String WINDOW_FROM_BIND = "wf";
  private static final String WINDOW_TO_BIND = "wt";

  private final String name;
  private final CassandraClient client;
  private final WindowSegmentPartitioner partitioner;

  private final PreparedStatement createSegment;
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
  private final PreparedStatement fetchStreamTime;
  private final PreparedStatement setStreamTime;
  private final PreparedStatement fetchEpoch;
  private final PreparedStatement reserveEpoch;
  private final PreparedStatement ensureEpoch;

  public static CassandraWindowedTable create(
      final CassandraTableSpec spec,
      final CassandraClient client,
      final WindowSegmentPartitioner partitioner
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

    final var createSegment = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(SEGMENT_ID.column(), bindMarker(SEGMENT_ID.bind()))
            .value(ROW_TYPE.column(), METADATA_ROW.literal())
            .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
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

    final var setOffset = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .build()
    );

    final var fetchStreamTime = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(STREAM_TIME.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(SEGMENT_ID.relation().isEqualTo(bindMarker(SEGMENT_ID.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(METADATA_TS)))
            .build()
    );

    final var setStreamTime = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(STREAM_TIME.column(), bindMarker(STREAM_TIME.bind()))
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

    return new CassandraWindowedTable(
        name,
        client,
        partitioner,
        createSegment,
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
        fetchStreamTime,
        setStreamTime,
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
        .withColumn(EPOCH.column(), DataTypes.BIGINT)
        .withColumn(STREAM_TIME.column(), DataTypes.BIGINT)
        .withCompaction(new DefaultLeveledCompactionStrategy()); // TODO: create a LCSTableSpec?
  }

  public CassandraWindowedTable(
      final String name,
      final CassandraClient client,
      final WindowSegmentPartitioner partitioner,
      final PreparedStatement createSegment,
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
      final PreparedStatement fetchStreamTime,
      final PreparedStatement setStreamTime,
      final PreparedStatement fetchEpoch,
      final PreparedStatement reserveEpoch,
      final PreparedStatement ensureEpoch
  ) {
    this.name = name;
    this.client = client;
    this.partitioner = partitioner;
    this.createSegment = createSegment;
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
    this.fetchStreamTime = fetchStreamTime;
    this.setStreamTime = setStreamTime;
    this.fetchEpoch = fetchEpoch;
    this.reserveEpoch = reserveEpoch;
    this.ensureEpoch = ensureEpoch;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CassandraWindowFlushManager init(
      final int kafkaPartition
  ) {
    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);

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

    final long epoch = fetchEpoch(metadataPartition) + 1;
    final var reserveMetadataEpoch = client.execute(reserveEpoch(metadataPartition, epoch));
    if (!reserveMetadataEpoch.wasApplied()) {
      handleEpochFencing(kafkaPartition, metadataPartition, epoch);
    }

    final long streamTime = fetchStreamTime(kafkaPartition);
    LOG.info("Initialized stream-time to {} with epoch {} for kafka partition {}",
             streamTime, epoch, kafkaPartition);

    // since the active data segments depend on the current stream-time for the windowed table,
    // which we won't know until we initialize it from the remote, the metadata like epoch and
    // stream-time are stored in a special metadata partition/segment that's separate from the
    // regular data partitions/segments and never expired
    // therefore we initialize from the metadata partition and then broadcast the epoch to
    // all the other partitions containing data for active segments
    final List<SegmentPartition> activeSegments =
        partitioner.segmenter().activeSegments(kafkaPartition, streamTime);
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

    return new CassandraWindowFlushManager(
        this,
        client,
        partitioner,
        kafkaPartition,
        epoch,
        streamTime
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

  public RemoteWriteResult<SegmentPartition> createSegment(
      final int kafkaPartition,
      final long epoch,
      final SegmentPartition segmentPartition
  ) {
    // TODO: use executeAsync to create and reserve epoch for segments
    final var createSegment = client.execute(createSegment(segmentPartition, epoch));

    // If the segment creation failed because the table partition already exists, attempt to
    // update the epoch in case we are fencing an older writer -- if that fails it means we're
    // the ones being fenced
    if (!createSegment.wasApplied()) {
      final var reserveEpoch = client.execute(reserveEpoch(segmentPartition, epoch));

      if (!reserveEpoch.wasApplied()) {
        return RemoteWriteResult.failure(segmentPartition);
      }
    }
    return RemoteWriteResult.success(segmentPartition);
  }

  private BoundStatement createSegment(final SegmentPartition segmentToCreate, final long epoch) {
    return createSegment
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentToCreate.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentToCreate.segmentId)
        .setLong(EPOCH.bind(), epoch)
        .setIdempotent(true);
  }

  public RemoteWriteResult<SegmentPartition> deleteSegment(
      final int kafkaPartition,
      final SegmentPartition segmentPartition
  ) {
    // TODO: use executeAsync
    final var expireSegmentResult = client.execute(
        expireSegment(segmentPartition)
    );

    if (!expireSegmentResult.wasApplied()) {
      return RemoteWriteResult.failure(segmentPartition);
    }

    return RemoteWriteResult.success(segmentPartition);
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

  public BoundStatement setOffset(
      final int kafkaPartition,
      final long offset
  ) {
    LOG.debug("{}[{}] Updating offset to {}", name, kafkaPartition, offset);

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
      final long epoch,
      final long streamTime
  ) {
    LOG.debug("{}[{}] Updating stream time to {} with epoch {}",
              name, kafkaPartition, streamTime, epoch);

    final SegmentPartition metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    return setStreamTime
        .bind()
        .setInt(PARTITION_KEY.bind(), metadataPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), metadataPartition.segmentId)
        .setLong(STREAM_TIME.bind(), streamTime);
  }

  /**
   * @param segmentPartition the table partition to fetch the epoch for
   *
   * @return the current epoch associated with this table partition
   */
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

  public BoundStatement reserveEpoch(final SegmentPartition segmentPartition, final long epoch) {
    return reserveEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setLong(EPOCH.bind(), epoch);
  }

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
    final SegmentPartition segmentPartition = partitioner.tablePartition(kafkaPartition, key);
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), segmentPartition.tablePartition)
        .setLong(SEGMENT_ID.bind(), segmentPartition.segmentId)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.key.get()))
        .setInstant(WINDOW_START.bind(), Instant.ofEpochMilli(key.windowStartMs));
  }

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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.segmenter().range(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.segmenter().reverseRange(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.segmenter().range(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.segmenter().reverseRange(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final SegmentPartition partition : partitioner.segmenter().range(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    final List<KeyValueIterator<WindowedKey, byte[]>> segmentIterators = new LinkedList<>();
    for (final var partition : partitioner.segmenter().reverseRange(
        kafkaPartition,
        timeFrom,
        timeTo
    )) {
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

  private static KeyValue<WindowedKey, byte[]> windowRows(final Row row) {
    final long startTs = row.getInstant(WINDOW_START.column()).toEpochMilli();
    final Bytes key = Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array());

    return new KeyValue<>(
        new WindowedKey(key, startTs),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }

}
