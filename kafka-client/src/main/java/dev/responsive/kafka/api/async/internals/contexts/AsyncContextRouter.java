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

package dev.responsive.kafka.api.async.internals.contexts;

import dev.responsive.kafka.api.async.internals.AsyncThread;
import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * A simple wrapper layer that routes processor context accesses that occur during
 * the user's #process method when executed by a thread from the async pool to the
 * async thread's specific instance of the AsyncProcessorContext. Each execution thread
 * in the async pool gets its own async context and the mapping is final and immutable
 * once the async pool is initialized, allowing lock-free/contention-free routing and
 * processor context access
 */
public class AsyncContextRouter<KOut, VOut> implements InternalProcessorContext<KOut, VOut> {

  private final Map<String, AsyncThreadProcessorContext<KOut, VOut>> threadToContext;
  private final StreamThreadProcessorContext<KOut, VOut> streamThreadProcessorContext;

  public AsyncContextRouter(
      final Map<String, AsyncThreadProcessorContext<KOut, VOut>> threadToContext,
      final StreamThreadProcessorContext<KOut, VOut> streamThreadProcessorContext
  ) {
    // Unmodifiable map ensures thread safety since we only do reads
    this.threadToContext = Collections.unmodifiableMap(threadToContext);

    this.streamThreadProcessorContext = streamThreadProcessorContext;
  }

  private AsyncThreadProcessorContext<KOut, VOut> lookupContextForCurrentThread() {
    if (Thread.currentThread() instanceof AsyncThread) {
      return (AsyncThreadProcessorContext<KOut, VOut>) ((AsyncThread) Thread.currentThread()).context();
    }
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    lookupContextForCurrentThread().forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record, final String childName) {
    lookupContextForCurrentThread().forward(record, childName);
  }

  @Override
  public String applicationId() {
    return lookupContextForCurrentThread().applicationId();
  }

  @Override
  public TaskId taskId() {
    return lookupContextForCurrentThread().taskId();
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return lookupContextForCurrentThread().recordMetadata();
  }

  @Override
  public Serde<?> keySerde() {
    return lookupContextForCurrentThread().keySerde();
  }

  @Override
  public Serde<?> valueSerde() {
    return lookupContextForCurrentThread().valueSerde();
  }

  @Override
  public File stateDir() {
    return lookupContextForCurrentThread().stateDir();
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    return lookupContextForCurrentThread().getStateStore(name);
  }

  @Override
  public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
    return lookupContextForCurrentThread().schedule(interval, type, callback);
  }

  @Override
  public void commit() {
    lookupContextForCurrentThread().commit();
  }

  @Override
  public Map<String, Object> appConfigs() {
    return lookupContextForCurrentThread().appConfigs();
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(final String prefix) {
    return lookupContextForCurrentThread().appConfigsWithPrefix(prefix);
  }

  @Override
  public long currentSystemTimeMs() {
    return lookupContextForCurrentThread().currentSystemTimeMs();
  }

  @Override
  public long currentStreamTimeMs() {
    return lookupContextForCurrentThread().currentStreamTimeMs();
  }
}
