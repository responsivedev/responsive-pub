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
