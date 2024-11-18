/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
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

