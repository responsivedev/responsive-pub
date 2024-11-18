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
import dev.responsive.kafka.internal.db.mongo.MongoSessionTable;
import dev.responsive.kafka.internal.db.mongo.MongoWriter;
import dev.responsive.kafka.internal.db.mongo.SessionDoc;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.SessionKey;
import java.util.function.Function;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class MongoSessionFlushManager extends SessionFlushManager {

  private final String logPrefix;
  private final Logger log;

  private final MongoSessionTable table;
  private final Function<SegmentPartition, MongoCollection<SessionDoc>> segmentToCollection;

  private final SessionSegmentPartitioner partitioner;
  private final int kafkaPartition;

  public MongoSessionFlushManager(
      final MongoSessionTable table,
      final Function<SegmentPartition, MongoCollection<SessionDoc>> segmentToCollection,
      final SessionSegmentPartitioner partitioner,
      final int kafkaPartition,
      final long streamTime
  ) {
    super(table.name(), kafkaPartition, partitioner.segmenter(), streamTime);

    this.table = table;
    this.segmentToCollection = segmentToCollection;
    this.partitioner = partitioner;
    this.kafkaPartition = kafkaPartition;

    logPrefix = String.format(
        "%s[%d] session-store {epoch=%d} ",
                              table.name(), kafkaPartition, table.localEpoch(kafkaPartition));
    log = new LogContext(logPrefix).logger(MongoSessionFlushManager.class);
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public TablePartitioner<SessionKey, SegmentPartition> partitioner() {
    return partitioner;
  }

  @Override
  public RemoteWriter<SessionKey, SegmentPartition> createWriter(final SegmentPartition segment) {
    log.debug("Creating writer for segment {}", segment);

    return new MongoWriter<>(
        table,
        kafkaPartition,
        segment,
        () -> segmentToCollection.apply(segment)
    );
  }

  @Override
  public RemoteWriteResult<SegmentPartition> createSegment(
      final SegmentPartition segmentPartition
  ) {
    return table.createSegmentForPartition(kafkaPartition, segmentPartition);
  }

  @Override
  public RemoteWriteResult<SegmentPartition> deleteSegment(
      final SegmentPartition segmentPartition
  ) {
    return table.deleteSegmentForPartition(kafkaPartition, segmentPartition);
  }

  @Override
  public String failedFlushInfo(
      final long batchOffset,
      final SegmentPartition failedTablePartition
  ) {
    return String.format("<batchOffset=%d, persistedOffset=%d>, <localEpoch=%d, persistedEpoch=%d>",
                         batchOffset, table.fetchOffset(kafkaPartition),
                         table.localEpoch(kafkaPartition), table.fetchEpoch(kafkaPartition));
  }

  @Override
  public RemoteWriteResult<SegmentPartition> updateOffsetAndStreamTime(
      final long consumedOffset,
      final long streamTime
  ) {
    try {
      // TODO: should we check result.wasAcknowledged()/use a write concern?
      table.setOffsetAndStreamTime(kafkaPartition, consumedOffset, streamTime);
    } catch (final MongoBulkWriteException e) {
      log.warn("Bulk write operation failed", e);
      final WriteConcernError writeConcernError = e.getWriteConcernError();
      if (writeConcernError != null) {
        log.warn("Bulk write operation failed due to write concern error {}", writeConcernError);
      } else {
        log.warn("Bulk write operation failed due to error(s): {}", e.getWriteErrors());
      }
      return RemoteWriteResult.failure(partitioner.metadataTablePartition(kafkaPartition));
    } catch (final MongoException e) {
      log.error("Unexpected exception running the bulk write operation", e);
      throw new RuntimeException("Bulk write operation failed", e);
    }
    return RemoteWriteResult.success(null);
  }

  public String logPrefix() {
    return logPrefix;
  }
}
