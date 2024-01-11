/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
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

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import org.apache.kafka.common.utils.Bytes;

public class CassandraFactFlushManager implements FlushManager<Bytes, Integer> {

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
    logPrefix = String.format("%s[%d] CassandraFactFlushManager", table.name(), kafkaPartition);
  }

  @Override
  public TablePartitioner<Bytes, Integer> partitioner() {
    return partitioner;
  }

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(final Integer tablePartition) {
    return new FactSchemaWriter<>(
        client,
        table,
        kafkaPartition,
        tablePartition
    );
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    return RemoteWriteResult.success(null);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
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
