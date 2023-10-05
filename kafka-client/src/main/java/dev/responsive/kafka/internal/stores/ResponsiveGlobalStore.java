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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.RemoteKeyValueSchema;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.SharedClients;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
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
  private static final int IGNORED_PARTITION = -1; // Global stores ignored partitions

  private final Logger log;

  private final ResponsiveKeyValueParams params;
  private final TableName name;
  private final Position position; // TODO(IQ): update the position during restoration

  private GlobalProcessorContextImpl context;

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
      final SharedClients sharedClients = SharedClients.loadSharedClients(context.appConfigs());
      client = sharedClients.cassandraClient;

      schema = client.prepareGlobalSchema(params);
      log.info("Global table {} is available for querying.", name);

      schema.init(name.cassandraName(), SubPartitioner.NO_SUBPARTITIONS, IGNORED_PARTITION);

      open = true;

      context.register(root, new RestoreCallback());
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
    put(key, value, context.partition(), context.offset(), context.timestamp());
  }

  private void put(
      final Bytes key,
      final byte[] value,
      final int partition,
      final long offset,
      final long timestamp
  ) {
    if (value == null) {
      delete(key);
    } else {
      putInternal(key, value, partition, offset, timestamp);
    }
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    final byte[] old = get(key);
    if (old == null && value != null) {
      putInternal(key, value, context.partition(), context.offset(), context.timestamp());
    }
    return old;
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
    entries.forEach(kv -> put(kv.key, kv.value));
  }

  private void putInternal(
      final Bytes key,
      final byte[] value,
      final int partition,
      final long offset,
      final long timestamp
  ) {
    client.execute(schema.insert(name.cassandraName(), IGNORED_PARTITION, key, value, timestamp));
    client.execute(schema.setOffset(name.cassandraName(), partition, offset));
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] delete(final Bytes key) {
    return delete(key, context.partition(), context.offset());
  }

  private byte[] delete(final Bytes key, final int partition, final long offset) {
    final byte[] bytes = get(key);
    client.execute(schema.delete(name.cassandraName(), IGNORED_PARTITION, key));
    client.execute(schema.setOffset(name.cassandraName(), partition, offset));

    // TODO: this position may be incorrectly updated during restoration
    StoreQueryUtils.updatePosition(position, context);
    return bytes;
  }

  @Override
  public byte[] get(final Bytes key) {
    return schema.get(name.cassandraName(), IGNORED_PARTITION, key, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    return schema.range(name.cassandraName(), IGNORED_PARTITION, from, to, ALL_VALID_TS);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return schema.all(name.cassandraName(), IGNORED_PARTITION, ALL_VALID_TS);
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return client.count(name.cassandraName(), IGNORED_PARTITION);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  private class RestoreCallback implements RecordBatchingStateRestoreCallback {

    @Override
    public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
      for (final var rec : records) {
        // We should consider using the same strategy we use in the
        // ResponsiveRestoreConsumer and just seek to where we need to
        // start from instead of skipping records - this is just less wiring
        // code for now and the number of records to skip is pretty minimal
        // (limited by the number of records committed between a single window
        // of auto.commit.interval.ms) unlike the changelog equivalent which
        // always restores from scratch
        final int partition = rec.partition();
        final long offset = schema.metadata(name.cassandraName(), partition).offset;
        if (rec.offset() < offset) {
          // ignore records that have already been processed - race conditions
          // are not important since the worst case we'll have just not written
          // the last record, which we just re-process here (we update the offset
          // after each write to remote)
          continue;
        }

        if (rec.value() == null) {
          delete(new Bytes(rec.key()), rec.partition(), rec.offset());
        } else {
          put(new Bytes(rec.key()), rec.value(), rec.partition(), rec.offset(), rec.timestamp());
        }
      }
    }
  }
}
