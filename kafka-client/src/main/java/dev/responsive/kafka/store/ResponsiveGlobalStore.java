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

import static dev.responsive.kafka.clients.SharedClients.loadSharedClients;

import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.ResponsiveKeyValueParams;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.utils.TableName;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

/**
 * The {@code ResponsiveGlobalStore}
 */
public class ResponsiveGlobalStore implements KeyValueStore<Bytes, byte[]> {

  private static final long ALL_VALID_TS = -1L; // Global stores don't support TTL

  private final Logger log;

  private final ResponsiveKeyValueParams params;
  private final TableName name;
  private final Position position; // TODO(IQ): update the position during restoration

  private GlobalProcessorContextImpl context;
  private int partition;

  private CassandraClient client;
  private RemoteKeyValueSchema schema;

  private boolean open;

  public ResponsiveGlobalStore(final ResponsiveKeyValueParams params) {
    this.params = params;
    this.name = params.name();
    this.position = Position.emptyPosition();
    log = new LogContext(
        String.format("global-store [%s]", name.kafkaName())
    ).logger(ResponsivePartitionedStore.class);
  }

  @Override
  public String name() {
    return name.kafkaName();
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
      log.info("Initializing global state store {}", name);
      this.context = (GlobalProcessorContextImpl) context;
      // this is bad, but the assumption is that global tables are small
      // and can fit in a single partition - all writers will write using
      // the same partition key. we should consider using ALLOW FILTERING
      // instead. that would make gets/puts more efficient and the queries
      // to be more evently distributed and take the hit only on range
      // queries
      partition = 0;
      final SharedClients sharedClients = loadSharedClients(context.appConfigs());
      client = sharedClients.cassandraClient;

      schema = client.prepareKVTableSchema(params);
      log.info("Global table {} is available for querying.", name);

      schema.init(name.cassandraName(), SubPartitioner.NO_SUBPARTITIONS, partition);

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
    client.execute(schema.insert(name.cassandraName(), partition, key, value, context.timestamp()));
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
  }

  @Override
  public byte[] delete(final Bytes key) {
    final byte[] bytes = get(key);
    client.execute(schema.delete(name.cassandraName(), partition, key));
    StoreQueryUtils.updatePosition(position, context);
    return bytes;
  }

  @Override
  public byte[] get(final Bytes key) {
    return schema.get(name.cassandraName(), partition, key, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return schema.range(name.cassandraName(), partition, from, to, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return schema.all(name.cassandraName(), partition, ALL_VALID_TS);
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return client.count(name.cassandraName(), partition);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }
}
