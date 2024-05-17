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
import java.util.List;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.TaskId;
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
    this(key, value, new TaskId(0, 0));
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final TaskId taskId
  ) {
    this(key, value, taskId, "topic");
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final TaskId taskId,
      final String topic
  ) {
    this(key, value, "async-processor", taskId, topic, 0L, 0L);
  }

  public AsyncTestEvent(
      final String key,
      final String value,
      final String asyncProcessorName,
      final TaskId taskId,
      final String topic,
      final long timestamp,
      final long offset
  ) {
    super(
        String.format("event <%s, %s>[%d]", key, value, taskId.partition()),
        new Record<>(key, value, timestamp),
        asyncProcessorName,
        taskId,
        new ProcessorRecordContext(
            timestamp,
            offset,
            taskId.partition(),
            topic,
            new RecordHeaders()
        ),
        0L,
        0L,
        () -> {},
        List.of()
    );
  }
}
