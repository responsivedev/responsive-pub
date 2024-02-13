/*
 * Copyright 2024 Responsive Computing, Inc.
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

import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TTDWriter<K, P> implements RemoteWriter<K, P> {
  private final TTDTable<K> table;
  private final P tablePartition;

  public TTDWriter(final TTDTable<K> table, final P tablePartition) {
    this.table = table;
    this.tablePartition = tablePartition;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void insert(final K key, final byte[] value, long epochMillis) {
    table.insert(0, key, value, epochMillis);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void delete(final K key) {
    table.delete(0, key);
  }

  @Override
  public CompletionStage<RemoteWriteResult<P>> flush() {
    return CompletableFuture.completedStage(RemoteWriteResult.success(tablePartition));
  }

}

