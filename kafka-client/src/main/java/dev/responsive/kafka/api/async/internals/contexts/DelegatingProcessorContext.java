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

package dev.responsive.kafka.api.async.internals.contexts;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * Basic wrapper around a {@link ProcessorContext}/{@link FixedKeyProcessorContext}
 * that just delegates to the underlying context.
 */
@SuppressWarnings({"checkstyle:linelength", "checkstyle:overloadmethodsdeclarationorder"})
public abstract class DelegatingProcessorContext<KOut, VOut, D extends ProcessorContext<KOut, VOut> & FixedKeyProcessorContext<KOut, VOut>>
    implements MergedProcessorContext<KOut, VOut> {

  public abstract D delegate();

  @Override
  public String applicationId() {
    return delegate().applicationId();
  }

  @Override
  public TaskId taskId() {
    return delegate().taskId();
  }

  @Override
  public Optional<RecordMetadata> recordMetadata() {
    return delegate().recordMetadata();
  }

  @Override
  public Serde<?> keySerde() {
    return delegate().keySerde();
  }

  @Override
  public Serde<?> valueSerde() {
    return delegate().valueSerde();
  }

  @Override
  public File stateDir() {
    return delegate().stateDir();
  }

  @Override
  public StreamsMetrics metrics() {
    return delegate().metrics();
  }

  @Override
  public <S extends StateStore> S getStateStore(final String name) {
    return delegate().getStateStore(name);
  }

  @Override
  public Cancellable schedule(
      final Duration interval,
      final PunctuationType type,
      final Punctuator callback
  ) {
    return delegate().schedule(interval, type, callback);
  }

  @Override
  public void commit() {
    delegate().commit();
  }

  @Override
  public Map<String, Object> appConfigs() {
    return delegate().appConfigs();
  }

  @Override
  public Map<String, Object> appConfigsWithPrefix(final String prefix) {
    return delegate().appConfigsWithPrefix(prefix);
  }

  @Override
  public long currentSystemTimeMs() {
    return delegate().currentSystemTimeMs();
  }

  @Override
  public long currentStreamTimeMs() {
    return delegate().currentStreamTimeMs();
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final FixedKeyRecord<K, V> record) {
    delegate().forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final FixedKeyRecord<K, V> record,
      final String childName
  ) {
    delegate().forward(record, childName);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(final Record<K, V> record) {
    delegate().forward(record);
  }

  @Override
  public <K extends KOut, V extends VOut> void forward(
      final Record<K, V> record,
      final String childName
  ) {
    delegate().forward(record, childName);
  }
}
