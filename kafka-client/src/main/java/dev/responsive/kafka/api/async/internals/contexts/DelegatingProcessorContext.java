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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ThreadCache;

/**
 * Basic wrapper around a {@link ProcessorContext}/{@link FixedKeyProcessorContext}
 * that just delegates to the underlying context.
 */
@SuppressWarnings({"checkstyle:linelength", "checkstyle:overloadmethodsdeclarationorder"})
public abstract class DelegatingProcessorContext<KOut, VOut, D
    extends InternalProcessorContext<KOut, VOut> & FixedKeyProcessorContext<KOut, VOut>>
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
  public StreamsMetricsImpl metrics() {
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

  @Override
  public <T extends StateStore> T getStateStore(final StoreBuilder<T> builder) {
    return delegate().getStateStore(builder);
  }

  @Override
  public void setSystemTimeMs(final long timeMs) {
    delegate().setSystemTimeMs(timeMs);
  }

  @Override
  public ProcessorRecordContext recordContext() {
    return delegate().recordContext();
  }

  @Override
  public void setRecordContext(final ProcessorRecordContext recordContext) {
    delegate().setRecordContext(recordContext);
  }

  @Override
  public void setCurrentNode(final ProcessorNode<?, ?, ?, ?> currentNode) {
    delegate().setCurrentNode(currentNode);
  }

  @Override
  public ProcessorNode<?, ?, ?, ?> currentNode() {
    return delegate().currentNode();
  }

  @Override
  public ThreadCache cache() {
    return delegate().cache();
  }

  @Override
  public void initialize() {
    delegate().initialize();
  }

  @Override
  public void uninitialize() {
    delegate().uninitialize();
  }

  @Override
  public Task.TaskType taskType() {
    return delegate().taskType();
  }

  @Override
  public void transitionToActive(
      final StreamTask streamTask,
      final RecordCollector recordCollector,
      final ThreadCache newCache
  ) {
    delegate().transitionToActive(streamTask, recordCollector, newCache);
  }

  @Override
  public void transitionToStandby(final ThreadCache newCache) {
    delegate().transitionToStandby(newCache);
  }

  @Override
  public void registerCacheFlushListener(
      final String namespace,
      final ThreadCache.DirtyEntryFlushListener listener
  ) {
    delegate().registerCacheFlushListener(namespace, listener);
  }

  @Override
  public void logChange(
      final String storeName,
      final Bytes key,
      final byte[] value,
      final long timestamp,
      final Position position
  ) {
    delegate().logChange(storeName, key, value, timestamp, position);
  }

  @Override
  public String changelogFor(final String storeName) {
    return delegate().changelogFor(storeName);
  }

  @Override
  public void addProcessorMetadataKeyValue(final String key, final long value) {
    delegate().addProcessorMetadataKeyValue(key, value);
  }

  @Override
  public Long processorMetadataForKey(final String key) {
    return delegate().processorMetadataForKey(key);
  }

  @Override
  public void setProcessorMetadata(final ProcessorMetadata metadata) {
    delegate().setProcessorMetadata(metadata);
  }

  @Override
  public ProcessorMetadata getProcessorMetadata() {
    return delegate().getProcessorMetadata();
  }

  @Override
  public void register(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
    delegate().register(store, stateRestoreCallback);
  }

  @Override
  public <K, V> void forward(final K key, final V value) {
    delegate().forward(key, value);
  }

  @Override
  public <K, V> void forward(final K key, final V value, final To to) {
    delegate().forward(key, value, to);
  }

  @Override
  public String topic() {
    return delegate().topic();
  }

  @Override
  public int partition() {
    return delegate().partition();
  }

  @Override
  public long offset() {
    return delegate().offset();
  }

  @Override
  public Headers headers() {
    return delegate().headers();
  }

  @Override
  public long timestamp() {
    return delegate().timestamp();
  }

  @Override
  public void register(
      final StateStore store,
      final StateRestoreCallback stateRestoreCallback,
      final CommitCallback commitCallback
  ) {
    delegate().register(store, stateRestoreCallback, commitCallback);
  }
}
