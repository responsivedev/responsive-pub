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

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TTDFlushManager<K> implements FlushManager<K, Integer> {

  private final String logPrefix;
  private final TTDTable<K> table;
  private final int kafkaPartition = 0; // dummy value as there are no real partitions in the TTD

  public TTDFlushManager(
      final TTDTable<K> table
  ) {
    this.table = table;
    this.logPrefix = String.format("%s TTDFlushManager ", table.name());
  }

  @Override
  public TablePartitioner<K, Integer> partitioner() {
    return TablePartitioner.defaultPartitioner();
  }

  @Override
  public RemoteWriter<K, Integer> createWriter(final Integer tablePartition) {
    return new TTDWriter<>(table, tablePartition);
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    return RemoteWriteResult.success(kafkaPartition);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
    return RemoteWriteResult.success(kafkaPartition);
  }

  private static class TTDWriter<K> implements RemoteWriter<K, Integer> {
    private final TTDTable<K> table;
    private final int tablePartition;

    public TTDWriter(final TTDTable<K> table, final int tablePartition) {
      this.table = table;
      this.tablePartition = tablePartition;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void insert(final K key, final byte[] value, long epochMillis) {
      table.insert(tablePartition, key, value, epochMillis);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void delete(final K key) {
      table.delete(tablePartition, key);
    }

    @Override
    public CompletionStage<RemoteWriteResult<Integer>> flush() {
      return CompletableFuture.completedStage(RemoteWriteResult.success(tablePartition));
    }

  }

  @Override
  public String logPrefix() {
    return logPrefix;
  }

}
