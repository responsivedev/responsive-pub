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

package dev.responsive.kafka.internal.db.mongo;

import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MongoWriter<K> implements RemoteWriter<K> {

  private final RemoteTable<K> table;
  private final int partition;

  public MongoWriter(final RemoteTable<K> table, final int partition) {
    this.table = table;
    this.partition = partition;
  }

  @Override
  public void insert(final K key, final byte[] value, final long epochMillis) {
    table.insert(partition, key, value, epochMillis);
  }

  @Override
  public void delete(final K key) {
    table.delete(partition, key);
  }

  @Override
  public CompletionStage<RemoteWriteResult> flush() {
    return CompletableFuture.completedFuture(RemoteWriteResult.success(partition));
  }

  @Override
  public RemoteWriteResult setOffset(final long offset) {
    table.setOffset(partition, offset);
    return RemoteWriteResult.success(partition);
  }
}
