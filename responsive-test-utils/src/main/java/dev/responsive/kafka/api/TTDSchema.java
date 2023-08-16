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

package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.MetadataRow;
import dev.responsive.db.RemoteSchema;
import dev.responsive.db.RemoteWriter;
import dev.responsive.db.WriterFactory;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.store.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Time;

public abstract class TTDSchema<K> implements RemoteSchema<K> {

  private final CassandraClientStub client;
  protected final Time time;

  public TTDSchema(final CassandraClientStub client) {
    this.client = client;
    this.time = client.time();
  }

  /**
   * @return the number of elements in this table
   *         or 0 if the schema has no such table
   */
  public abstract long count(final String tableName);

  @Override
  public void prepare(final String tableName) {

  }

  @Override
  public WriterFactory<K> init(
      final String tableName,
      final SubPartitioner partitioner,
      final int kafkaPartition
  ) {
    return (client, name, partition, batchSize) -> new TTDWriter<K>(this, tableName, partition);
  }

  @Override
  public MetadataRow metadata(final String table, final int partition) {
    return new MetadataRow(0, 0);
  }

  @Override
  public BoundStatement setOffset(final String table, final int partition, final long offset) {
    return null;
  }

  @Override
  public CassandraClient cassandraClient() {
    return client;
  }

  private static class TTDWriter<K> implements RemoteWriter<K> {
    private final TTDSchema<K> schema;
    private final String tableName;
    private final int partition;

    public TTDWriter(final TTDSchema<K> schema, final String tableName, final int partition) {
      this.schema = schema;
      this.tableName = tableName;
      this.partition = partition;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void insert(final K key, final byte[] value) {
      schema.insert(tableName, partition, key, value);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void delete(final K key) {
      schema.delete(tableName, partition, key);
    }

    @Override
    public CompletionStage<RemoteWriteResult> flush() {
      return CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public RemoteWriteResult setOffset(final long offset) {
      schema.setOffset(tableName, partition, offset);
      return RemoteWriteResult.success(partition);
    }

    @Override
    public int partition() {
      return partition;
    }
  }

}
