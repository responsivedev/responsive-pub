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

import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class AsyncKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private final KeyValueStore<K, V> userDelegate;

  public AsyncKeyValueStore(final ProcessorContext<?, ?> userContext, final String name) {
    this.userDelegate = userContext.getStateStore(name);
  }

  public V get(final K key) {

  }

  public void put(K key, V value) {

  }

  @Override
  public V putIfAbsent(final K key, final V value) {
    final V oldValue = get(key);
    if (oldValue == null) {
      put(key, value);
    }
    return oldValue;
  }

  @Override
  public void putAll(final List<KeyValue<K, V>> entries) {
    for (final KeyValue<K, V> kv : entries) {
      put(kv.key, kv.value);
    }
  }

  @Override
  public V delete(final K key) {
    final V oldValue = get(key);
    put(key, null);
    return oldValue;
  }


  @Override
  public String name() {
    return userDelegate.name();
  }

  @Override
  @Deprecated
  public void init(
      final org.apache.kafka.streams.processor.ProcessorContext context,
      final StateStore root
  ) {
    throw new UnsupportedOperationException("This init method is deprecated, please implement"
                                                + "init(StateStoreContext, StateStore) instead");
  }

  @Override
  public void init(final StateStoreContext context, final StateStore root) {
    userDelegate.init(context, root);
  }

  @Override
  public void flush() {
    // TODO: how should we handle this, particularly in the ALOS case where it might be
    //  called as part of regular processing?
    userDelegate.flush();
  }

  @Override
  public void close() {
    userDelegate.close();
  }

  @Override
  public boolean persistent() {
    return userDelegate.persistent();
  }

  @Override
  public boolean isOpen() {
    return userDelegate.isOpen();
  }

  @Override
  public <R> QueryResult<R> query(final Query<R> query, final PositionBound positionBound, final QueryConfig config) {
    throw new UnsupportedOperationException("IQv2 not yet supported with async processing");
  }

  @Override
  public Position getPosition() {
    return userDelegate.getPosition();
  }

  @Override
  public KeyValueIterator<K, V> range(final K from, final K to) {
    throw new UnsupportedOperationException("#range is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<K, V> reverseRange(final K from, final K to) {
    throw new UnsupportedOperationException("#reverseRange is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<K, V> all() {
    throw new UnsupportedOperationException("#all is not yet supported with async processing");
  }

  @Override
  public KeyValueIterator<K, V> reverseAll() {
    throw new UnsupportedOperationException("#reverseAll is not yet supported with async processing");
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(final P prefix, final PS prefixKeySerializer) {
    throw new UnsupportedOperationException("#prefixScan is not yet supported with async processing");
  }

  @Override
  public long approximateNumEntries() {
    return userDelegate.approximateNumEntries();
  }
}
