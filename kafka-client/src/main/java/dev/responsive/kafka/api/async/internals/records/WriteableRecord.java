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

import dev.responsive.kafka.api.async.internals.AsyncProcessorRecordContext;
import dev.responsive.kafka.api.async.internals.queues.WritingQueue;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A record (or tombstone) that should be written to the given state store.
 * These records are created by the AsyncThread and handed off to the StreamThread
 * for execution. See {@link WritingQueue} for more details
 */
public class WriteableRecord<KS, VS> implements AsyncRecord<KS, VS> {

  private final Record<KS, VS> record;
  private final String storeName;
  private final AsyncProcessorRecordContext recordContext;
  private final Runnable putListener;

  public WriteableRecord(
      final Record<KS, VS> record,
      final String storeName,
      final AsyncProcessorRecordContext recordContext,
      final Runnable putListener
  ) {
    this.record = record;
    this.storeName = storeName;
    this.recordContext = recordContext;
    this.putListener = putListener;
  }

  public Record<KS, VS> record() {
    return record;
  }

  public String storeName() {
    return storeName;
  }

  public int partition() {
    return recordContext.partition();
  }

  @Override
  public KS key() {
    return record.key();
  }

  @Override
  public VS value() {
    return record.value();
  }

  @Override
  public String topic() {
    return recordContext.topic();
  }

  @Override
  public long offset() {
    return recordContext.offset();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WriteableRecord<?, ?> that = (WriteableRecord<?, ?>) o;

    if (!record.equals(that.record)) {
      return false;
    }
    if (!storeName.equals(that.storeName)) {
      return false;
    }
    return recordContext.equals(that.recordContext);
  }

  @Override
  public int hashCode() {
    int result = record.hashCode();
    result = 31 * result + storeName.hashCode();
    result = 31 * result + recordContext.hashCode();
    return result;
  }
}
