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

package dev.responsive.kafka.testutils;

import dev.responsive.kafka.api.async.internals.events.AsyncEvent;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * Simple utility wrapper for creating AsyncEvents with only the parameters
 * the test is actually concerned with, and everything else stubbed in
 */
public class AsyncTestEvent extends AsyncEvent {

  public AsyncTestEvent(
      final String key,
      final String value
  ) {
    this(key, value, 0);
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final int partition
  ) {
    this(key, value, partition, "topic");
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final int partition,
      final String topic
  ) {
    this(key, value, partition, topic, 0L, 0L);
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final int partition,
      final String topic,
      final long timestamp,
      final long offset
  ) {
    super(
        String.format("event <%s, %s>[%d]", key, value, partition),
        new Record<>(key, value, timestamp),
        partition,
        new ProcessorRecordContext(
            timestamp,
            offset,
            partition,
            topic,
            new RecordHeaders()
        ),
        0L,
        0L,
        () -> {}
    );
  }
}
