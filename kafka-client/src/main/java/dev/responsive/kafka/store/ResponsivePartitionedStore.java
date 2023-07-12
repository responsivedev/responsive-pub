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
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.StoreUtil;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
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
import org.slf4j.LoggerFactory;

public class ResponsivePartitionedStore implements KeyValueStore<Bytes, byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsivePartitionedStore.class);

  // visible for testing
  static final Plugin PLUGIN = new Plugin();

  private CassandraClient client;

  private final TableName name;
  private final Position position;
  private ResponsiveStoreRegistry storeRegistry;
  private ResponsiveStoreRegistration registration;

  private boolean open;
  private SubPartitioner partitioner;
  private CommitBuffer<Bytes> buffer;

  @SuppressWarnings("rawtypes")
  private InternalProcessorContext context;
  private int partition;

  public ResponsivePartitionedStore(
      final TableName name
  ) {
    this.name = name;
    this.position = Position.emptyPosition();
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
      LOG.info("Initializing state store {}", name);
      StoreUtil.validateTopologyOptimizationConfig(context.appConfigs());
      final ResponsiveConfig config = new ResponsiveConfig(context.appConfigs());
      this.context = asInternalProcessorContext(context);
      partition = context.taskId().partition();

      final SharedClients sharedClients = new SharedClients(context.appConfigs());
      client = sharedClients.cassandraClient;

      final RemoteMonitor monitor = client.awaitTable(name.cassandraName(), sharedClients.executor);
      client.createDataTable(name.cassandraName());
      monitor.await(Duration.ofSeconds(60));
      LOG.info("Remote table {} is available for querying.", name.cassandraName());

      client.prepareStatements(name.cassandraName());

      final TopicPartition topicPartition =  new TopicPartition(
          changelogFor(context, name.kafkaName(), false),
          partition
      );
      partitioner = config.getSubPartitioner(
          client, sharedClients.admin, name, topicPartition.topic());

      buffer = new CommitBuffer<>(
          client,
          name.cassandraName(),
          topicPartition,
          sharedClients.admin,
          PLUGIN,
          StoreUtil.shouldTruncateChangelog(
              topicPartition.topic(),
              sharedClients.admin,
              context.appConfigs()
          ),
          FlushTriggers.fromConfig(config),
          partitioner
      );
      buffer.init();

      open = true;

      context.register(root, buffer);
      final long offset = buffer.offset();
      registration = new ResponsiveStoreRegistration(
          name.kafkaName(),
          topicPartition,
          offset == -1 ? 0 : offset,
          buffer::flush
      );
      storeRegistry = InternalConfigs.loadStoreRegistry(context.appConfigs());
      storeRegistry.registerStore(registration);
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
    buffer.put(key, value);
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
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] delete(final Bytes key) {
    // single writer prevents races (see putIfAbsent)
    final byte[] old = get(key);
    buffer.tombstone(key);
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

    return client.get(name.cassandraName(), partitioner.partition(partition, key), key);
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
        .all(partition)
        .mapToLong(p -> client.count(name.cassandraName(), p))
        .sum();
  }

  @Override
  public void close() {
    if (storeRegistry != null) {
      storeRegistry.deregisterStore(registration);
    }
    // the client is shared across state stores, so only the
    // buffer needs to be flushed
    flush();
  }

  private static class Plugin implements BufferPlugin<Bytes> {

    @Override
    public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
      return Bytes.wrap(record.key());
    }

    @Override
    public BoundStatement insertData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Bytes key,
        final byte[] value
    ) {
      return client.insertData(tableName, partition, key, value);
    }

    @Override
    public BoundStatement deleteData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Bytes key
    ) {
      return client.deleteData(tableName, partition, key);
    }

    @Override
    public Bytes bytes(final Bytes key) {
      return key;
    }

    @Override
    public int compare(final Bytes o1, final Bytes o2) {
      return o1.compareTo(o2);
    }
  }
}
