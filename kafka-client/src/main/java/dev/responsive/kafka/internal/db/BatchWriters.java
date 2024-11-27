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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Prepares a writer for each table partition that will be written to when the
 * current batch of pending writes are flushed
 */
public class BatchWriters<K, P> {
  private final int kafkaPartition;
  private final FlushManager<K, P> flushManager;
  private final Map<P, RemoteWriter<K, P>> batchWriters = new HashMap<>();

  public BatchWriters(
      final FlushManager<K, P> flushManager,
      final int kafkaPartition
  ) {
    this.kafkaPartition = kafkaPartition;
    this.flushManager = flushManager;
  }

  public int numTablePartitionsInBatch() {
    return batchWriters.size();
  }

  public RemoteWriter<K, P> findOrAddWriter(
      final K key,
      final long consumedOffset
  ) {
    flushManager.writeAdded(key);

    final P tablePartition = flushManager.partitioner().tablePartition(kafkaPartition, key);
    return batchWriters.computeIfAbsent(
        tablePartition,
        tp -> flushManager.createWriter(tp, consumedOffset)
    );
  }

  public Collection<RemoteWriter<K, P>> allWriters() {
    return batchWriters.values();
  }

}
