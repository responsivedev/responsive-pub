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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;

public class CassandraKVFlushManager extends KVFlushManager {

  private final String logPrefix;
  private final CassandraKeyValueTable table;
  private final CassandraClient client;

  private final TablePartitioner<Bytes, Integer> partitioner;
  private final int kafkaPartition;
  private final long epoch;

  public CassandraKVFlushManager(
      final CassandraKeyValueTable table,
      final CassandraClient client,
      final TablePartitioner<Bytes, Integer> partitioner,
      final int kafkaPartition,
      final long epoch
  ) {
    this.table = table;
    this.client = client;
    this.partitioner = partitioner;
    this.kafkaPartition = kafkaPartition;
    this.epoch = epoch;

    logPrefix = String.format("%s[%d] kv-store {epoch=%d} ", table.name(), kafkaPartition, epoch);
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public TablePartitioner<Bytes, Integer> partitioner() {
    return partitioner;
  }

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(final Integer tablePartition) {
    return new LwtWriter<>(
        client,
        () -> table.ensureEpoch(tablePartition, epoch),
        table,
        kafkaPartition,
        tablePartition
    );
  }

  @Override
  public String failedFlushInfo(final long batchOffset, final Integer failedTablePartition) {
    return String.format("<batchOffset=%d, persistedOffset=%d>, <localEpoch=%d, persistedEpoch=%d>",
                         batchOffset, table.fetchOffset(kafkaPartition),
                         epoch, table.fetchEpoch(failedTablePartition));
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(final long consumedOffset) {
    final int tablePartition = partitioner.metadataTablePartition(kafkaPartition);

    final BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.UNLOGGED);
    builder.addStatement(table.ensureEpoch(tablePartition, epoch));
    builder.addStatement(table.setOffset(kafkaPartition, consumedOffset));

    final var result = client.execute(builder.build());
    return result.wasApplied()
        ? RemoteWriteResult.success(tablePartition)
        : RemoteWriteResult.failure(tablePartition);
  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }
}
