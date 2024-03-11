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
import org.apache.kafka.streams.processor.api.Record;

public class ScheduleableRecord<KIn, VIn> implements AsyncRecord<KIn, VIn> {

  private final Record<KIn, VIn> record;
  private final AsyncProcessorRecordContext recordContext;

  public ScheduleableRecord(
      final Record<KIn, VIn> record,
      final AsyncProcessorRecordContext recordContext
  ) {
    this.record = record;
    this.recordContext = recordContext;
  }

  public Record<KIn, VIn> record() {
    return record;
  }

  public AsyncProcessorRecordContext recordContext() {
    return recordContext;
  }

  @Override
  public KIn key() {
    return record.key();
  }

  @Override
  public VIn value() {
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
}
