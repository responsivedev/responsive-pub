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
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class CassandraWindowFlushManager extends SegmentedWindowFlushManager {

  private final String logPrefix;
  private final Logger log;

  private final CassandraWindowTable table;
  private final CassandraClient client;

  private final TablePartitioner<WindowedKey, SegmentPartition> partitioner;
  private final int kafkaPartition;
  private final long epoch;

  public CassandraWindowFlushManager(
      final CassandraWindowTable table,
      final CassandraClient client,
      final WindowSegmentPartitioner partitioner,
      final int kafkaPartition,
      final long epoch,
      final long streamTime
  ) {
    super(table.name(), kafkaPartition, partitioner.segmenter(), streamTime);
    this.table = table;
    this.client = client;
    this.partitioner = partitioner;
    this.kafkaPartition = kafkaPartition;
    this.epoch = epoch;

    logPrefix = String.format("%s[%d] window-store {epoch=%s} ",
                              table.name(), kafkaPartition, epoch);
    log = new LogContext(logPrefix).logger(CassandraWindowFlushManager.class);
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public TablePartitioner<WindowedKey, SegmentPartition> partitioner() {
    return partitioner;
  }

  @Override
  public RemoteWriter<WindowedKey, SegmentPartition> createWriter(
      final SegmentPartition tablePartition,
      final long consumedOffset
  ) {
    return new LwtWriter<>(
        client,
        () -> table.ensureEpoch(tablePartition, epoch),
        table,
        kafkaPartition,
        tablePartition
    );
  }

  @Override
  public RemoteWriteResult<SegmentPartition> createSegment(
      final SegmentPartition segmentPartition
  ) {
    return table.createSegment(kafkaPartition, epoch, segmentPartition);
  }

  @Override
  public RemoteWriteResult<SegmentPartition> deleteSegment(
      final SegmentPartition segmentPartition
  ) {
    return table.deleteSegment(kafkaPartition, segmentPartition);
  }

  @Override
  public String failedFlushInfo(
      final long batchOffset,
      final SegmentPartition failedTablePartition
  ) {
    return String.format("<batchOffset=%d, persistedOffset=%d>, <localEpoch=%d, persistedEpoch=%d>",
                         batchOffset, table.lastWrittenOffset(kafkaPartition),
                         epoch, table.fetchEpoch(failedTablePartition));
  }

  @Override
  public RemoteWriteResult<SegmentPartition> updateOffsetAndStreamTime(
      final long consumedOffset,
      final long streamTime
  ) {
    final SegmentPartition metadataSegment = partitioner.metadataTablePartition(kafkaPartition);

    final BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.UNLOGGED);
    builder.addStatement(table.ensureEpoch(metadataSegment, epoch));
    builder.addStatement(table.setOffset(kafkaPartition, consumedOffset));
    builder.addStatement(table.setStreamTime(kafkaPartition, epoch, streamTime));

    final var result = client.execute(builder.build());
    if (!result.wasApplied()) {
      return RemoteWriteResult.failure(metadataSegment);
    }

    return RemoteWriteResult.success(metadataSegment);
  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }
}
