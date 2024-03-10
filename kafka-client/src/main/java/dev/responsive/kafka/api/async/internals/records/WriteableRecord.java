/*
 * Copyright 2024 Responsive Computing, Inc.
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

package dev.responsive.kafka.api.async.internals.records;

import dev.responsive.kafka.api.async.internals.queues.WritingQueue;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A record (or tombstone) that should be written to the given state store.
 * These records are created by the AsyncThread and handed off to the StreamThread
 * for execution. See {@link WritingQueue} for more details
 */
public class WriteableRecord<K, V> {

  private final Record<K, V> record;
  private final String storeName;
  private final int partition;

  public WriteableRecord(final Record<K, V> record, final String storeName, final int partition) {
    this.record = record;
    this.storeName = storeName;
    this.partition = partition;
  }

  public Record<K, V> record() {
    return record;
  }

  public String storeName() {
    return storeName;
  }

  public int partition() {
    return partition;
  }
}
