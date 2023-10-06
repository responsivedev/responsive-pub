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

package dev.responsive.kafka.internal.stores;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.clients.TTDCassandraClient;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.MetadataRow;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Time;

public abstract class TTDTable<K> implements RemoteTable<K> {

  private final TTDCassandraClient client;
  protected final Time time;

  public TTDTable(final TTDCassandraClient client) {
    this.client = client;
    this.time = client.time();
  }

  /**
   * @return the number of elements in this table
   *         or 0 if the schema has no such table
   */
  public abstract long count();

  @Override
  public WriterFactory<K> init(
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    return (client, partition, batchSize) -> new TTDWriter<K>(this, partition);
  }

  @Override
  public MetadataRow metadata(final int partition) {
    return new MetadataRow(0, 0);
  }

  @Override
  public BoundStatement setOffset(final int partition, final long offset) {
    return null;
  }

  @Override
  public CassandraClient cassandraClient() {
    return client;
  }

  private static class TTDWriter<K> implements RemoteWriter<K> {
    private final TTDTable<K> table;
    private final int partition;

    public TTDWriter(final TTDTable<K> table, final int partition) {
      this.table = table;
      this.partition = partition;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void insert(final K key, final byte[] value, long epochMillis) {
      table.insert(partition, key, value, epochMillis);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void delete(final K key) {
      table.delete(partition, key);
    }

    @Override
    public CompletionStage<RemoteWriteResult> flush() {
      return CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public RemoteWriteResult setOffset(final long offset) {
      table.setOffset(partition, offset);
      return RemoteWriteResult.success(partition);
    }

  }

}
