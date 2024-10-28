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
import static dev.responsive.kafka.internal.db.ColumnName.TIMESTAMP;
import static dev.responsive.kafka.internal.db.RowType.DATA_ROW;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.Iterators;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyValueTable implements RemoteKVTable<BoundStatement> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueTable.class);
  private static final String FROM_BIND = "fk";
  private static final String TO_BIND = "tk";

  private final String name;
  private final CassandraClient client;
  private final SubPartitioner partitioner;
  private final Optional<TtlResolver<?, ?>> ttlResolver;

  private final PreparedStatement get;
  private final PreparedStatement range;
  private final PreparedStatement all;
  private final PreparedStatement insert;
  private final PreparedStatement delete;
  private final PreparedStatement fetchOffset;
  private final PreparedStatement setOffset;
  private final PreparedStatement fetchEpoch;
  private final PreparedStatement reserveEpoch;
  private final PreparedStatement ensureEpoch;

  public static CassandraKeyValueTable create(
      final RemoteTableSpec spec,
      final CassandraClient client
  ) throws InterruptedException, TimeoutException {
    final String name = spec.tableName();
    final var ttlResolver = spec.ttlResolver();
    LOG.info("Creating data table {} in remote store.", name);
    client.execute(spec.applyDefaultOptions(createTable(name, ttlResolver)).build());

    client.awaitTable(name).await(Duration.ofSeconds(60));

    final var insert = client.prepare(
        QueryBuilder
            .insertInto(name)
            .value(PARTITION_KEY.column(), bindMarker(PARTITION_KEY.bind()))
            .value(ROW_TYPE.column(), DATA_ROW.literal())
            .value(DATA_KEY.column(), bindMarker(DATA_KEY.bind()))
            .value(TIMESTAMP.column(), bindMarker(TIMESTAMP.bind()))
            .value(DATA_VALUE.column(), bindMarker(DATA_VALUE.bind()))
            .build(),
        QueryOp.WRITE
    );

    final var get = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_VALUE.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build(),
        QueryOp.READ
    );

    final var range = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(DATA_KEY.relation().isGreaterThanOrEqualTo(bindMarker(FROM_BIND)))
            .where(DATA_KEY.relation().isLessThanOrEqualTo(bindMarker(TO_BIND)))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build(),
        QueryOp.READ
    );

    final var all = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .columns(DATA_KEY.column(), DATA_VALUE.column(), TIMESTAMP.column())
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(TIMESTAMP.relation().isGreaterThanOrEqualTo(bindMarker(TIMESTAMP.bind())))
            // ALLOW FILTERING is OK b/c the query only scans one partition
            .allowFiltering()
            .build(),
        QueryOp.READ
    );

    final var delete = client.prepare(
        QueryBuilder
            .deleteFrom(name)
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(DATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(bindMarker(DATA_KEY.bind())))
            .build(),
        QueryOp.WRITE
    );

    final var fetchOffset = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build(),
        QueryOp.READ
    );

    final var setOffset = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build(),
        QueryOp.WRITE
    );

    final var fetchEpoch = client.prepare(
        QueryBuilder
            .selectFrom(name)
            .column(EPOCH.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .build(),
        QueryOp.READ
    );

    final var reserveEpoch = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isLessThan(bindMarker(EPOCH.bind()))
            .build(),
        QueryOp.WRITE
    );

    final var ensureEpoch = client.prepare(
        QueryBuilder
            .update(name)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build(),
        QueryOp.WRITE
    );

    return new CassandraKeyValueTable(
        name,
        client,
        (SubPartitioner) spec.partitioner(),
        ttlResolver,
        get,
        range,
        all,
        insert,
        delete,
        fetchOffset,
        setOffset,
        fetchEpoch,
        reserveEpoch,
        ensureEpoch
    );
  }

  private static CreateTableWithOptions createTable(
      final String tableName,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) {
    final var baseOptions = SchemaBuilder
        .createTable(tableName)
        .ifNotExists()
        .withPartitionKey(PARTITION_KEY.column(), DataTypes.INT)
        .withClusteringColumn(ROW_TYPE.column(), DataTypes.TINYINT)
        .withClusteringColumn(DATA_KEY.column(), DataTypes.BLOB)
        .withColumn(DATA_VALUE.column(), DataTypes.BLOB)
        .withColumn(OFFSET.column(), DataTypes.BIGINT)
        .withColumn(EPOCH.column(), DataTypes.BIGINT)
        .withColumn(TIMESTAMP.column(), DataTypes.TIMESTAMP);

    if (ttlResolver.isPresent() && ttlResolver.get().defaultTtl().isFinite()) {
      final int defaultTtlSeconds = (int) ttlResolver.get().defaultTtl().toSeconds();
      return baseOptions.withDefaultTimeToLiveSeconds(defaultTtlSeconds);
    } else {
      return baseOptions;
    }
  }

  // Visible for Testing
  public CassandraKeyValueTable(
      final String name,
      final CassandraClient client,
      final SubPartitioner partitioner,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final PreparedStatement get,
      final PreparedStatement range,
      final PreparedStatement all,
      final PreparedStatement insert,
      final PreparedStatement delete,
      final PreparedStatement fetchOffset,
      final PreparedStatement setOffset,
      final PreparedStatement fetchEpoch,
      final PreparedStatement reserveEpoch,
      final PreparedStatement ensureEpoch
  ) {
    this.name = name;
    this.client = client;
    this.partitioner = partitioner;
    this.ttlResolver = ttlResolver;
    this.get = get;
    this.range = range;
    this.all = all;
    this.insert = insert;
    this.delete = delete;
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
  public CassandraKVFlushManager init(
      final int kafkaPartition
  ) {
    partitioner.allTablePartitions(kafkaPartition).forEach(tablePartition -> client.execute(
        QueryBuilder.insertInto(name)
            .value(PARTITION_KEY.column(), PARTITION_KEY.literal(tablePartition))
            .value(ROW_TYPE.column(), METADATA_ROW.literal())
            .value(DATA_KEY.column(), DATA_KEY.literal(METADATA_KEY))
            .value(TIMESTAMP.column(), TIMESTAMP.literal(METADATA_TS))
            .value(OFFSET.column(), OFFSET.literal(NO_COMMITTED_OFFSET))
            .value(EPOCH.column(), EPOCH.literal(0L))
            .ifNotExists()
            .build()
    ));

    // attempt to reserve an epoch - the epoch is only fetched from the metadata
    // table-partition and then "broadcast" to the other partitions.
    // this works because we are guaranteed that:
    // (a) for this kind of table,the metadata partition is included in the set of all
    // data table-partitions, and
    // (b) the same epoch is written to all table partitions (unless it was fenced by
    // another writer before reserving the epoch for all table-partitions, in which case
    // it doesn't matter because that writer will overwrite the epoch for all the
    // partitions to the same newer value
    final int metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final long epoch = fetchEpoch(metadataPartition) + 1;

    for (final int tablePartition : partitioner.allTablePartitions(kafkaPartition)) {
      final var setEpoch = client.execute(reserveEpoch(tablePartition, epoch));

      if (!setEpoch.wasApplied()) {
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
    }

    final int basePartition = partitioner.metadataTablePartition(kafkaPartition);
    LOG.info("Initialized store {} with epoch {} for subpartitions in range: {{} -> {}}",
             name, epoch, basePartition, basePartition + partitioner.getFactor() - 1);

    return new CassandraKVFlushManager(
        this,
        client,
        partitioner,
        kafkaPartition,
        epoch
    );
  }

  @Override
  public byte[] get(
      final int kafkaPartition,
      final Bytes key,
      final long streamTimeMs
  ) {
    final long minValidTs = ttlResolver.isEmpty()
        ? -1L
        : streamTimeMs - ttlResolver.get().defaultTtl().toMillis();

    final int tablePartition = partitioner.tablePartition(kafkaPartition, key);

    final BoundStatement get = this.get
        .bind()
        .setInt(PARTITION_KEY.bind(), tablePartition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

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
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    final long minValidTs = ttlResolver.isEmpty()
        ? -1L
        : streamTimeMs - ttlResolver.get().defaultTtl().toMillis();

    // TODO: explore more efficient ways to serve bounded range queries, for now we have to
    //  iterate over all subpartitions and merge the results since we don't know which subpartitions
    //  hold keys within the given range
    //  One option would be to configure the partitioner with an alternative hasher that's optimized
    //  for range queries with a Comparator-aware key-->subpartition mapping strategy.
    final List<KeyValueIterator<Bytes, byte[]>> resultsPerPartition = new LinkedList<>();
    for (final int partition : partitioner.allTablePartitions(kafkaPartition)) {
      final BoundStatement range = this.range
          .bind()
          .setInt(PARTITION_KEY.bind(), partition)
          .setByteBuffer(FROM_BIND, ByteBuffer.wrap(from.get()))
          .setByteBuffer(TO_BIND, ByteBuffer.wrap(to.get()))
          .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

      final ResultSet result = client.execute(range);
      resultsPerPartition.add(Iterators.kv(result.iterator(), CassandraKeyValueTable::rows));
    }
    return Iterators.wrapped(resultsPerPartition);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final int kafkaPartition,
      final long streamTimeMs
  ) {
    final long minValidTs = ttlResolver.isEmpty()
        ? -1L
        : streamTimeMs - ttlResolver.get().defaultTtl().toMillis();

    final List<KeyValueIterator<Bytes, byte[]>> resultsPerPartition = new LinkedList<>();
    for (final int partition : partitioner.allTablePartitions(kafkaPartition)) {
      final BoundStatement range = this.all
          .bind()
          .setInt(PARTITION_KEY.bind(), partition)
          .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(minValidTs));

      final ResultSet result = client.execute(range);
      resultsPerPartition.add(Iterators.kv(result.iterator(), CassandraKeyValueTable::rows));
    }
    return Iterators.wrapped(resultsPerPartition);
  }

  @Override
  public BoundStatement insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    final int tablePartition = partitioner.tablePartition(kafkaPartition, key);
    return insert
        .bind()
        .setInt(PARTITION_KEY.bind(), tablePartition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()))
        .setInstant(TIMESTAMP.bind(), Instant.ofEpochMilli(epochMillis))
        .setByteBuffer(DATA_VALUE.bind(), ByteBuffer.wrap(value));
  }

  @Override
  public BoundStatement delete(
      final int kafkaPartition,
      final Bytes key
  ) {
    final int tablePartition = partitioner.tablePartition(kafkaPartition, key);
    return delete
        .bind()
        .setInt(PARTITION_KEY.bind(), tablePartition)
        .setByteBuffer(DATA_KEY.bind(), ByteBuffer.wrap(key.get()));
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final int metadataTablePartition = partitioner.metadataTablePartition(kafkaPartition);

    final List<Row> result = client.execute(
        fetchOffset
            .bind()
            .setInt(PARTITION_KEY.bind(), metadataTablePartition))
        .all();

    if (result.size() > 1) {
      throw new IllegalStateException(String.format(
          "Expected at most one offset row for %s[%s] but got %d",
          name, kafkaPartition, result.size()));
    } else if (result.isEmpty()) {
      return NO_COMMITTED_OFFSET;
    } else {
      return result.get(0).getLong(OFFSET.column());
    }
  }

  public BoundStatement setOffset(final int kafkaPartition, final long offset) {
    final int metadataTablePartition = partitioner.metadataTablePartition(kafkaPartition);
    return setOffset
        .bind()
        .setInt(PARTITION_KEY.bind(), metadataTablePartition)
        .setLong(OFFSET.bind(), offset);
  }

  /**
   * @param tablePartition the table partition to fetch the epoch for
   *
   * @return the current epoch associated with this table partition
   */
  public long fetchEpoch(final Integer tablePartition) {
    final List<Row> result = client.execute(
            fetchEpoch
                .bind()
                .setInt(PARTITION_KEY.bind(), tablePartition))
        .all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one epoch metadata row for %s[%s] but got %d",
          name, tablePartition, result.size()));
    } else {
      return result.get(0).getLong(EPOCH.column());
    }
  }

  public BoundStatement reserveEpoch(final Integer tablePartition, final long epoch) {
    return reserveEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), tablePartition)
        .setLong(EPOCH.bind(), epoch);
  }

  public BoundStatement ensureEpoch(final Integer tablePartition, final long epoch) {
    return ensureEpoch
        .bind()
        .setInt(PARTITION_KEY.bind(), tablePartition)
        .setLong(EPOCH.bind(), epoch);
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    return partitioner.allTablePartitions(kafkaPartition)
        .stream()
        .mapToLong(tablePartition -> client.count(name(), tablePartition))
        .sum();
  }

  private static KeyValue<Bytes, byte[]> rows(final Row row) {
    return new KeyValue<>(
        Bytes.wrap(row.getByteBuffer(DATA_KEY.column()).array()),
        row.getByteBuffer(DATA_VALUE.column()).array()
    );
  }
}