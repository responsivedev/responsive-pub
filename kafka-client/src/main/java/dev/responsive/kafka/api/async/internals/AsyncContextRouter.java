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

package dev.responsive.kafka.api.async.internals;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * A simple wrapper layer that routes processor context accesses that occur during
 * the user's #process method when executed by a thread from the async pool to the
 * async thread's specific instance of the AsyncProcessorContext. Each execution thread
 * in the async pool gets its own async context and the mapping is final and immutable
 * once the async pool is initialized, allowing lock-free/contention-free routing and
 * processor context access
 */
public class AsyncContextRouter<KOut, VOut> implements ProcessorContext<KOut, VOut> {

  private final Map<String, AsyncProcessorContext<KOut, VOut>> asyncThreadToContext;

  public AsyncContextRouter(
      final Map<String, AsyncProcessorContext<KOut, VOut>> asyncThreadToContext
  ) {
    this.asyncThreadToContext = Collections.unmodifiableMap(asyncThreadToContext);
  }

  public AsyncProcessorContext<KOut, VOut> getThreadContext(final String asyncThreadName) {
    return asyncThreadToContext.get(asyncThreadName);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    asyncThreadToContext.get(Thread.currentThread().getName()
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record, final String childName) {

  }

  @Override
  public String applicationId() {
    return null;
  }

  @Override
  public TaskId taskId() {
    return null;
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return Optional.empty();
  }

  @Override
  public Serde<?> keySerde() {
    return null;
  }

  @Override
  public Serde<?> valueSerde() {
    return null;
  }

  @Override
  public File stateDir() {
    return null;
  }

  @Override
  public StreamsMetrics metrics() {
    return null;
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    return null;
  }

  @Override
  public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
    return null;
  }

  @Override
  public void commit() {

  }

  @Override
  public Map<String, Object> appConfigs() {
    return null;
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(final String prefix) {
    return null;
  }

  @Override
  public long currentSystemTimeMs() {
    return 0;
  }

  @Override
  public long currentStreamTimeMs() {
    return 0;
  }
}
