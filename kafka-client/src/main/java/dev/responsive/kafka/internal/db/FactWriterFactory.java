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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.db.partitioning.ResponsivePartitioner;
import dev.responsive.kafka.internal.db.partitioning.ResponsivePartitioner.DefaultPartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;

public class FactWriterFactory<K> extends WriterFactory<K, Integer> {

  private final RemoteTable<K, BoundStatement> table;
  private final CassandraClient client;
  private final ResponsivePartitioner<K, Integer> partitioner;
  private final int kafkaPartition;

  public FactWriterFactory(
      final RemoteTable<K, BoundStatement> table,
      final CassandraClient client,
      final int kafkaPartition
  ) {
    super(String.format("FactWriterFactory [%s-%d] ", table.name(), kafkaPartition));
    this.table = table;
    this.client = client;
    this.partitioner = new DefaultPartitioner<>();
    this.kafkaPartition = kafkaPartition;
  }

  @Override
  public RemoteWriter<K, Integer> createWriter(
      final Integer tablePartition
  ) {
    return new FactSchemaWriter<>(
        client,
        table,
        tablePartition
    );
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public Integer tablePartitionForKey(final K key) {
    return partitioner.tablePartition(kafkaPartition, key);
  }

  @Override
  public RemoteWriteResult<Integer> setOffset(final long offset) {
    final int metadataPartition = partitioner.metadataTablePartition(kafkaPartition);
    final var result = client.execute(table.setOffset(kafkaPartition, offset));
    return result.wasApplied()
        ? RemoteWriteResult.success(metadataPartition)
        : RemoteWriteResult.failure(metadataPartition);
  }

  @Override
  protected long offset() {
    return table.fetchOffset(kafkaPartition);
  }

}
