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
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.db.BytesKeySpec;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.RemoteKeyValueSchema;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.api.ResponsiveKeyValueParams;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.kafka.store.SchemaTypes.KVSchema;
import dev.responsive.model.Result;
import dev.responsive.utils.TableName;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;

public class ResponsivePartitionedStore implements KeyValueStore<Bytes, byte[]> {

  private final Logger log;

  private final ResponsiveKeyValueParams params;
  private final TableName name;
  private final Position position; // TODO(IQ): update the position during restoration

  @SuppressWarnings("rawtypes")
  private InternalProcessorContext context;
  private TopicPartition partition;

  private CommitBuffer<Bytes, RemoteKeyValueSchema> buffer;
  private RemoteKeyValueSchema schema;
  private ResponsiveStoreRegistry storeRegistry;
  private ResponsiveStoreRegistration registration;
  private SubPartitioner partitioner;

  private boolean open;

  public ResponsivePartitionedStore(final ResponsiveKeyValueParams params) {
    this.params = params;
    this.name = params.name();
    this.position = Position.emptyPosition();
    log = new LogContext(
        String.format("store [%s]", name.kafkaName())
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
  public void init(final StateStoreContext storeContext, final StateStore root) {
    try {
      log.info("Initializing state store");
      context = asInternalProcessorContext(storeContext);

      final ResponsiveConfig config = ResponsiveConfig.quietConfig(storeContext.appConfigs());
      final SharedClients sharedClients = loadSharedClients(storeContext.appConfigs());
      final CassandraClient client = sharedClients.cassandraClient;

      storeRegistry = InternalConfigs.loadStoreRegistry(storeContext.appConfigs());
      partition =  new TopicPartition(
          changelogFor(storeContext, name.kafkaName(), false),
          context.taskId().partition()
      );
      partitioner = params.schemaType() == KVSchema.FACT
          ? SubPartitioner.NO_SUBPARTITIONS
          : config.getSubPartitioner(sharedClients.admin, name, partition.topic());

      schema = client.prepareKVTableSchema(params);
      log.info("Remote table {} is available for querying.", name.cassandraName());

      buffer = CommitBuffer.from(
          sharedClients,
          name,
          partition,
          schema,
          new BytesKeySpec(),
          partitioner,
          config
      );
      buffer.init();

      open = true;

      final long offset = buffer.offset();
      registration = new ResponsiveStoreRegistration(
          name.kafkaName(),
          partition,
          offset == -1 ? 0 : offset,
          buffer::flush
      );
      storeRegistry.registerStore(registration);

      storeContext.register(root, buffer);
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean persistent() {
    // Kafka Streams uses this to determine whether it
    // needs to create and lock state directories. since
    // the Responsive Client doesn't require flushing state
    // to disk, we return false even though the store is
    // persistent in a remote store
    return false;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    buffer.put(key, value, context.timestamp());
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    // since there's only a single writer, we don't need to worry
    // about the concurrency aspects here (e.g. it's not possible
    // that between the get(key) and put(key, value) another write
    // comes in unless this client is fenced, in which case this
    // batch will not be committed to remote storage)
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
    // single writer prevents races (see putIfAbsent)
    final byte[] old = get(key);
    buffer.tombstone(key, context.timestamp());
    StoreQueryUtils.updatePosition(position, context);
    return old;
  }

  @Override
  public byte[] get(final Bytes key) {
    // try the buffer first, it acts as a local cache
    // but this is also necessary for correctness as
    // it is possible that the data is either uncommitted
    // or not yet pushed to the remote store
    final Result<Bytes> result = buffer.get(key);
    if (result != null) {
      return result.isTombstone ? null : result.value;
    }

    final long minValidTs = params
        .timeToLive()
        .map(ttl -> context.timestamp() - ttl.toMillis())
        .orElse(-1L);

    return schema.get(
        name.cassandraName(),
        partitioner.partition(partition.partition(), key),
        key,
        minValidTs
    );
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Position getPosition() {
    return position;
  }

  @Override
  public long approximateNumEntries() {
    return partitioner
        .all(partition.partition())
        .mapToLong(p -> schema.cassandraClient().count(name.cassandraName(), p))
        .sum();
  }

  @Override
  public void close() {
    // no need to flush the buffer here, will happen through the kafka client commit as usual
    if (storeRegistry != null) {
      storeRegistry.deregisterStore(registration);
    }
  }
}
