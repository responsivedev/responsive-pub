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

import dev.responsive.db.CassandraClient;
import dev.responsive.utils.RemoteMonitor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;

/**
 * The {@code ResponsiveGlobalStore}
 */
public class ResponsiveGlobalStore implements KeyValueStore<Bytes, byte[]> {

  private final CassandraClient client;
  private final String name;
  private final String tableName;
  private final RemoteMonitor initRemote;
  private final Position position;

  private boolean open;
  private int partition;
  private StateStoreContext context;

  public ResponsiveGlobalStore(
      final CassandraClient client,
      final String name,
      final RemoteMonitor initRemote
  ) {
    this.client = client;
    this.name = name;
    this.tableName = '"' + name + '"';
    this.initRemote = initRemote;
    this.position = Position.emptyPosition();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @SuppressWarnings("deprecation")
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
    try {
      this.context = context;
      // this is bad, but the assumption is that global tables are small
      // and can fit in a single partition - all writers will write using
      // the same partition key. we should consider using ALLOW FILTERING
      // instead. that would make gets/puts more efficient and the queries
      // to be more evently distributed and take the hit only on range
      // queries
      partition = 0;
      client.createDataTable(tableName);
      initRemote.await(Duration.ofSeconds(60));

      client.prepareStatements(tableName);
      client.initializeOffset(tableName, partition);

      open = true;

      context.register(root, (key, value) -> {
        if (value == null) {
          delete(new Bytes(key));
        } else {
          put(new Bytes(key), value);
        }
      });
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean persistent() {
    return false;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    client.execute(client.insertData(tableName, partition, key, value));
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    final byte[] old = get(key);
    if (old == null) {
      put(key, value);
    }
    return old;
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] delete(final Bytes key) {
    final byte[] bytes = get(key);
    client.execute(client.deleteData(tableName, partition, key));
    return bytes;
  }

  @Override
  public byte[] get(final Bytes key) {
    return client.get(tableName, partition, key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return client.range(tableName, partition, from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return client.all(tableName, partition);
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return client.count(tableName, partition);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }
}
