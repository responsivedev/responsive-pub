/*
 *  Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
