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

package dev.responsive.kafka.internal.db.dynamo;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class DynamoKVFlushManager extends KVFlushManager {

  private final String logPrefix;
  private final Logger log;

  private final DynamoKVTable table;

  private final int kafkaPartition;
  private final DynamoDbAsyncClient dynamo;
  private final TablePartitioner<Bytes, Integer> partitioner;


  public DynamoKVFlushManager(
      final DynamoKVTable table,
      final int kafkaPartition,
      final DynamoDbAsyncClient dynamo
  ) {
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.dynamo = dynamo;

    partitioner = TablePartitioner.defaultPartitioner();
    logPrefix = String.format("%s[%d] kv-store {epoch=%d} ",
        table.name(), kafkaPartition, table.localEpoch(kafkaPartition));
    log = new LogContext(logPrefix).logger(DynamoKVFlushManager.class);
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
    return new DynamoWriter<>(table, kafkaPartition, tablePartition, dynamo);
  }

  @Override
  public String failedFlushInfo(final long batchOffset, final Integer failedTablePartition) {
    // TODO(agavra): add persisted epoch
    return String.format("<batchOffset=%d, persistedOffset=%d>, <localEpoch=%d, persistedEpoch=%d>",
        batchOffset, table.fetchOffset(kafkaPartition),
        table.localEpoch(kafkaPartition), 0);
  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(final long consumedOffset) {
    try {
      table.setOffset(kafkaPartition, consumedOffset);
      return RemoteWriteResult.success(kafkaPartition);
    } catch (final Exception e) {
      // TODO(agavra): improve error handling to be closer to MongoKVFlushManager
      log.error("Failed to update offset", e);
      return RemoteWriteResult.failure(kafkaPartition);
    }
  }
}
