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
import static dev.responsive.kafka.internal.db.ColumnName.EPOCH;
import static dev.responsive.kafka.internal.db.ColumnName.METADATA_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.PARTITION_KEY;
import static dev.responsive.kafka.internal.db.ColumnName.ROW_TYPE;
import static dev.responsive.kafka.internal.db.ColumnName.WINDOW_START;
import static dev.responsive.kafka.internal.db.RowType.METADATA_ROW;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.SessionClients;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LwtWriterFactory<K> implements WriterFactory<K> {

  private static final Logger LOG = LoggerFactory.getLogger(LwtWriterFactory.class);

  private final long epoch;
  private final PreparedStatement ensureEpoch;
  private final RemoteTable<K> table;

  public static <K> LwtWriterFactory<K> reserveWindowed(
      final RemoteTable<K> table,
      final CassandraClient client,
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    return reserve(table, client, partitioner, kafkaPartition, true);
  }

  public static <K> LwtWriterFactory<K> reserve(
      final RemoteTable<K> table,
      final CassandraClient client,
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    return reserve(table, client, partitioner, kafkaPartition, false);
  }

  private static <K> LwtWriterFactory<K> reserve(
      final RemoteTable<K> table,
      final CassandraClient client,
      final SubPartitioner partitioner,
      final int kafkaPartition,
      final boolean windowed
  ) {
    // attempt to reserve an epoch - all epoch reservations will be done
    // under the first sub-partition and then "broadcast" to the other
    // partitions
    final int basePartition = partitioner.first(kafkaPartition);
    final long epoch = table.metadata(basePartition).epoch + 1;
    return reserve(
        table,
        client,
        partitioner.all(kafkaPartition).toArray(),
        kafkaPartition,
        epoch,
        windowed
    );
  }

  // Visible for Testing
  public static <K> LwtWriterFactory<K> reserve(
      final RemoteTable<K> table,
      final CassandraClient client,
      final int[] partitions,
      final int kafkaPartition,
      final long epoch,
      final boolean windowed
  ) {
    for (final int sub : partitions) {
      final var setEpoch = windowed
          ? reserveEpochWindowed(table, client, sub, epoch)
          : reserveEpoch(table,  client, sub, epoch);

      if (!setEpoch.wasApplied()) {
        final long otherEpoch = table.metadata(sub).epoch;
        final var msg = String.format(
            "Could not initialize commit buffer %s[%d] - attempted to claim epoch %d, "
                + "but was fenced by a writer that claimed epoch %d on sub partition %d",
            table.name(),
            kafkaPartition,
            epoch,
            otherEpoch,
            sub
        );
        final var e = new TaskMigratedException(msg);
        LOG.warn(msg, e);
        throw e;
      }
    }

    return new LwtWriterFactory<>(
        table,
        epoch,
        windowed
            ? ensureEpochWindowed(table, client, epoch)
            : ensureEpoch(table, client, epoch)
    );
  }

  public LwtWriterFactory(
      final RemoteTable<K> table,
      final long epoch,
      final PreparedStatement ensureEpoch
  ) {
    this.table = table;
    this.epoch = epoch;
    this.ensureEpoch = ensureEpoch;
  }

  @Override
  public RemoteWriter<K> createWriter(
      final SessionClients client,
      final int partition,
      final int batchSize
  ) {
    return new LwtWriter<>(
        client.cassandraClient(),
        () -> ensureEpoch.bind().setInt(PARTITION_KEY.bind(), partition),
        table,
        partition,
        batchSize
    );
  }

  @Override
  public String toString() {
    return "LwtWriterFactory{"
        + "epoch=" + epoch
        + '}';
  }

  private static ResultSet reserveEpoch(
      final RemoteTable<?> table,
      final CassandraClient client,
      final int partition,
      final long epoch
  ) {
    return client.execute(
        QueryBuilder.update(table.name())
            .setColumn(EPOCH.column(), EPOCH.literal(epoch))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
            .ifColumn(EPOCH.column()).isLessThan(EPOCH.literal(epoch))
            .build()
    );
  }

  private static ResultSet reserveEpochWindowed(
      final RemoteTable<?> table,
      final CassandraClient client,
      final int partition,
      final long epoch
  ) {
    return client.execute(
        QueryBuilder.update(table.name())
            .setColumn(EPOCH.column(), EPOCH.literal(epoch))
            .where(PARTITION_KEY.relation().isEqualTo(PARTITION_KEY.literal(partition)))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(EPOCH.column()).isLessThan(EPOCH.literal(epoch))
            .build()
    );
  }

  private static PreparedStatement ensureEpoch(
      final RemoteTable<?> table,
      final CassandraClient client,
      final long epoch
  ) {
    return client.prepare(
        QueryBuilder.update(table.name())
            .setColumn(EPOCH.column(), EPOCH.literal(epoch))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .ifColumn(EPOCH.column()).isEqualTo(EPOCH.literal(epoch))
            .build(),
        QueryOp.WRITE
    );
  }

  private static PreparedStatement ensureEpochWindowed(
      final RemoteTable<?> table,
      final CassandraClient client,
      final long epoch
  ) {
    return client.prepare(
        QueryBuilder.update(table.name())
            .setColumn(EPOCH.column(), EPOCH.literal(epoch))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind())))
            .where(ROW_TYPE.relation().isEqualTo(METADATA_ROW.literal()))
            .where(DATA_KEY.relation().isEqualTo(DATA_KEY.literal(METADATA_KEY)))
            .where(WINDOW_START.relation().isEqualTo(WINDOW_START.literal(0L)))
            .ifColumn(EPOCH.column()).isEqualTo(EPOCH.literal(epoch))
            .build(),
        QueryOp.WRITE
    );
  }
}
