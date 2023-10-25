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
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TTDWriterFactory<K> extends WriterFactory<K, Integer> {

  private final RemoteTable<K, BoundStatement> table;
  private final int kafkaPartition;

  public TTDWriterFactory(
      final RemoteTable<K, BoundStatement> table,
      final int kafkaPartition
  ) {
    super(String.format("FactWriterFactory [%s-%d] ", table.name(), kafkaPartition));
    this.table = table;
    this.kafkaPartition = kafkaPartition;
  }

  @Override
  public RemoteWriter<K, Integer> createWriter(final Integer tablePartition) {
    return null;
  }

  @Override
  public String tableName() {
    return null;
  }

  @Override
  protected Integer tablePartitionForKey(final K key) {
    return 0;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public RemoteWriteResult<Integer> setOffset(final long offset) {
    table.setOffset(partition, offset);
    return RemoteWriteResult.success(partition);
  }

  @Override
  protected long offset() {
    return 0;
  }

  private static class TTDWriter<K> implements RemoteWriter<K, Integer> {
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
    public CompletionStage<RemoteWriteResult<Integer>> flush() {
      return CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    }

  }
}
