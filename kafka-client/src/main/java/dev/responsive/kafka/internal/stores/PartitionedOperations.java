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

import static dev.responsive.kafka.internal.utils.SharedClients.loadSharedClients;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SharedClients;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;

public class PartitionedOperations implements KeyValueOperations {

  private final ResponsiveKeyValueParams params;
  private final RemoteKVTable table;
  private final CommitBuffer<Bytes, RemoteKVTable> buffer;
  private final SubPartitioner partitioner;
  private final TopicPartition partition;
  @SuppressWarnings("rawtypes")
  private final InternalProcessorContext context;
  private final ResponsiveStoreRegistration registration;

  public static PartitionedOperations create(
      final TableName name,
      final StateStoreContext storeContext,
      final ResponsiveKeyValueParams params,
      final CassandraTableSpec spec
  ) throws InterruptedException, TimeoutException {
    final var log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(PartitionedOperations.class);
    final var context = asInternalProcessorContext(storeContext);

    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(storeContext.appConfigs());
    final SharedClients sharedClients = loadSharedClients(storeContext.appConfigs());
    final CassandraClient client = sharedClients.cassandraClient;

    final var partition =  new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );
    final var partitioner = params.schemaType() == SchemaTypes.KVSchema.FACT
        ? SubPartitioner.NO_SUBPARTITIONS
        : config.getSubPartitioner(sharedClients.admin, name, partition.topic());

    final RemoteKVTable table;
    switch (params.schemaType()) {
      case KEY_VALUE:
        table = client.kvFactory().create(spec);
        break;
      case FACT:
        table = client.factFactory().create(spec);
        break;
      default:
        throw new IllegalArgumentException("Unexpected schema type " + params.schemaType());
    }

    log.info("Remote table {} is available for querying.", name.cassandraName());

    final var buffer = CommitBuffer.from(
        sharedClients,
        partition,
        table,
        new BytesKeySpec(),
        params.truncateChangelog(),
        partitioner,
        config
    );
    buffer.init();

    final long offset = buffer.offset();
    final var registration = new ResponsiveStoreRegistration(
        name.kafkaName(),
        partition,
        offset == -1 ? 0 : offset,
        buffer::flush
    );
    return new PartitionedOperations(
        params,
        table,
        buffer,
        partitioner,
        partition,
        context,
        registration
    );
  }

  @SuppressWarnings("rawtypes")
  public PartitionedOperations(
      final ResponsiveKeyValueParams params,
      final RemoteKVTable table,
      final CommitBuffer<Bytes, RemoteKVTable> buffer,
      final SubPartitioner partitioner,
      final TopicPartition partition,
      final InternalProcessorContext context,
      final ResponsiveStoreRegistration registration
  ) {
    this.params = params;
    this.table = table;
    this.buffer = buffer;
    this.partitioner = partitioner;
    this.partition = partition;
    this.context = context;
    this.registration = registration;
  }

  @Override
  public void register(final ResponsiveStoreRegistry storeRegistry) {
    storeRegistry.registerStore(registration);
  }

  @Override
  public void deregister(final ResponsiveStoreRegistry storeRegistry) {
    storeRegistry.deregisterStore(registration);
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    buffer.put(key, value, context.timestamp());
  }

  @Override
  public byte[] delete(final Bytes key) {
    // single writer prevents races (see putIfAbsent)
    final byte[] old = get(key);
    buffer.tombstone(key, context.timestamp());

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

    return table.get(
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
  public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public long approximateNumEntries() {
    return partitioner
        .all(partition.partition())
        .mapToLong(p -> table.cassandraClient().count(table.name(), p))
        .sum();
  }

  @Override
  public void close() {
    // no need to flush the buffer here, will happen through the kafka client commit as usual
    buffer.close();
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    buffer.restoreBatch(records);
  }
}
