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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoCollection;
import dev.responsive.kafka.internal.db.mongo.KVDoc;
import dev.responsive.kafka.internal.db.mongo.MongoKVTable;
import dev.responsive.kafka.internal.db.mongo.MongoWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class MongoKVFlushManager extends KVFlushManager {
  private final String logPrefix;
  private final Logger log;

  private final MongoKVTable table;
  private final MongoCollection<KVDoc> kvDocs;

  private final TablePartitioner<Bytes, Integer> partitioner;
  private final int kafkaPartition;

  public MongoKVFlushManager(
      final MongoKVTable table,
      final MongoCollection<KVDoc> kvDocs,
      final int kafkaPartition
  ) {
    this.table = table;
    this.kvDocs = kvDocs;
    this.kafkaPartition = kafkaPartition;

    partitioner = TablePartitioner.defaultPartitioner();
    logPrefix = String.format("%s[%d] kv-store {epoch=%d} ",
                              table.name(), kafkaPartition, table.localEpoch(kafkaPartition));
    log = new LogContext(logPrefix).logger(MongoKVFlushManager.class);
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
    return new MongoWriter<>(table, kafkaPartition, tablePartition, () -> kvDocs);
  }

  @Override
  public String failedFlushInfo(final long batchOffset, final Integer failedTablePartition) {
    return String.format("<batchOffset=%d, persistedOffset=%d>, <localEpoch=%d, persistedEpoch=%d>",
                         batchOffset, table.lastWrittenOffset(kafkaPartition),
                         table.localEpoch(kafkaPartition), table.fetchEpoch(kafkaPartition));
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(final long consumedOffset) {
    try {
      // TODO: should we check result.wasAcknowledged()/use a write concern?
      table.setOffset(kafkaPartition, consumedOffset);
    } catch (final MongoBulkWriteException e) {
      log.warn("Bulk write operation failed", e);
      final WriteConcernError writeConcernError = e.getWriteConcernError();
      if (writeConcernError != null) {
        log.warn("Bulk write operation failed due to write concern error {}", writeConcernError);
      } else {
        log.warn("Bulk write operation failed due to error(s): {}", e.getWriteErrors());
      }
      return RemoteWriteResult.failure(kafkaPartition);
    } catch (final MongoException e) {
      log.error("Unexpected exception running the bulk write operation", e);
      throw new RuntimeException("Bulk write operation failed", e);
    }
    return RemoteWriteResult.success(kafkaPartition);
  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }
}
