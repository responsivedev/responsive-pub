/*
 *
 *  * Copyright 2024 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoCollection;
import dev.responsive.kafka.internal.db.mongo.KVDoc;
import dev.responsive.kafka.internal.db.mongo.MetadataDoc;
import dev.responsive.kafka.internal.db.mongo.MongoKVTable;
import dev.responsive.kafka.internal.db.mongo.MongoWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class MongoKVFlushManager implements FlushManager<Bytes, Integer> {
  private final String logPrefix;
  private final Logger log;

  private final MongoKVTable table;
  private final MongoCollection<KVDoc> kvDocs;
  private final MongoCollection<MetadataDoc> metadataDocs;

  private final TablePartitioner<Bytes, Integer> partitioner;
  private final int kafkaPartition;

  public MongoKVFlushManager(
      final MongoKVTable table,
      final MongoCollection<KVDoc> kvDocs,
      final MongoCollection<MetadataDoc> metadataDocs,
      final int kafkaPartition
  ) {
    this.table = table;
    this.kvDocs = kvDocs;
    this.metadataDocs = metadataDocs;
    this.kafkaPartition = kafkaPartition;

    partitioner = TablePartitioner.defaultPartitioner();
    logPrefix = String.format("%s[%d] MongoKVFlushManager {epoch=%d} ",
                              table.name(), kafkaPartition, table.epoch(kafkaPartition));
    log = new LogContext(logPrefix).logger(MongoKVFlushManager.class);
  }

  @Override
  public TablePartitioner<Bytes, Integer> partitioner() {
    return partitioner;
  }

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(final Integer tablePartition) {
    return new MongoWriter<>(table, kafkaPartition, tablePartition, () -> kvDocs);
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    return RemoteWriteResult.success(kafkaPartition);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
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
