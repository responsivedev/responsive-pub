/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.store;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import dev.responsive.utils.TableName;
import java.util.List;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class ResponsiveStore implements KeyValueStore<Bytes, byte[]> {

  private final TableName name;
  private KeyValueStore<Bytes, byte[]> delegate;

  public ResponsiveStore(
      final TableName name
  ) {
    this.name = name;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    delegate.put(key, value);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    return delegate.putIfAbsent(key, value);
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    delegate.putAll(entries);
  }

  @Override
  public byte[] delete(final Bytes key) {
    return delegate.delete(key);
  }

  @Override
  public String name() {
    // used before initialization
    return name.kafkaName();
  }

  @Override
  @Deprecated
  public void init(final ProcessorContext context, final StateStore root) {
    if (context instanceof StateStoreContext) {
      init((StateStoreContext) context, root);
    } else {
      throw new UnsupportedOperationException(
          "Use ResponsiveStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext context, final StateStore root) {
    if (asInternalProcessorContext(context).taskType() == TaskType.GLOBAL) {
      delegate = new ResponsiveGlobalStore(name);
    } else {
      delegate = new ResponsivePartitionedStore(name);
    }

    delegate.init(context, root);
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public boolean persistent() {
    // used before initialization
    return false;
  }

  @Override
  public boolean isOpen() {
    // used before initialization
    return delegate != null && delegate.isOpen();
  }

  @Override
  @Evolving
  public <R> QueryResult<R> query(
      final Query<R> query,
      final PositionBound positionBound,
      final QueryConfig config
  ) {
    return delegate.query(query, positionBound, config);
  }

  @Override
  @Evolving
  public Position getPosition() {
    return delegate.getPosition();
  }

  @Override
  public byte[] get(final Bytes key) {
    return delegate.get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return delegate.range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
    return delegate.reverseRange(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return delegate.all();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    return delegate.reverseAll();
  }

  @Override
  public <S extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(
      final P prefix,
      final S prefixKeySerializer
  ) {
    return delegate.prefixScan(prefix, prefixKeySerializer);
  }

  @Override
  public long approximateNumEntries() {
    return delegate.approximateNumEntries();
  }

  // Visible for testing
  KeyValueStore<Bytes, byte[]> getDelegate() {
    return delegate;
  }
}
