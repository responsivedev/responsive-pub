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

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;

public class CassandraFactFlushManager extends KVFlushManager {

  private final String logPrefix;
  private final CassandraFactTable table;
  private final CassandraClient client;

  private final TablePartitioner<Bytes, Integer> partitioner;
  private final int kafkaPartition;

  public CassandraFactFlushManager(
      final CassandraFactTable table,
      final CassandraClient client,
      final int kafkaPartition
  ) {
    this.table = table;
    this.client = client;
    this.kafkaPartition = kafkaPartition;

    partitioner = TablePartitioner.defaultPartitioner();
    logPrefix = String.format("%s[%d] fact-store", table.name(), kafkaPartition);
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
  public RemoteWriter<Bytes, Integer> createWriter(
      final Integer tablePartition,
      final long consumedOffset
  ) {
    return new FactSchemaWriter<>(
        client,
        table,
        kafkaPartition,
        tablePartition
    );
  }

  @Override
  public String failedFlushInfo(final long batchOffset, final Integer failedTablePartition) {
    return String.format("<batchOffset=%d, persistedOffset=%d>",
                         batchOffset, table.lastWrittenOffset(kafkaPartition));
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(final long consumedOffset) {
    final int metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final var result = client.execute(table.setOffset(kafkaPartition, consumedOffset));
    return result.wasApplied()
        ? RemoteWriteResult.success(metadataPartition)
        : RemoteWriteResult.failure(metadataPartition);
  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }
}
