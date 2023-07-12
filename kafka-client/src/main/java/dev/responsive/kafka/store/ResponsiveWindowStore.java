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
import static org.apache.kafka.streams.state.StateSerdes.TIMESTAMP_SIZE;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.api.InternalConfigs;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.model.Stamped;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.RemoteMonitor;
import dev.responsive.utils.StoreUtil;
import dev.responsive.utils.TableName;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollector.Supplier;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveWindowStore implements WindowStore<Bytes, byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveWindowStore.class);

  private CassandraClient client;

  private final TableName name;
  private final long windowSize;
  private final Position position;
  private final long retentionPeriod;

  private boolean open;

  @SuppressWarnings("rawtypes")
  private InternalProcessorContext context;
  private int partition;
  private CommitBuffer<Stamped<Bytes>> buffer;
  private long observedStreamTime;
  private ResponsiveStoreRegistry registry;
  private ResponsiveStoreRegistration registration;
  private SubPartitioner partitioner;

  public ResponsiveWindowStore(
      final TableName name,
      final long retentionPeriod,
      final long windowSize,
      final boolean retainDuplicates
  ) {
    this.name = name;

    // TODO: figure out how to implement retention period in Cassandra
    // there are a few options for this: we can use the wall-clock based
    // TTL feature, but this would be a departure from how Kafka Streams
    // handles the retention period (based on stream time). Alternatively
    // we can post-filter from Cassandra and occasionally run a process
    // that cleans up expired records, this would work well in a background
    // process but we'd need to figure out where to run that - we could also
    // run these deletes asynchronously
    //
    // for now (so we can get correct behavior) we just post-filter anything
    // that is past the TTL
    this.retentionPeriod = retentionPeriod;
    this.windowSize = windowSize;
    this.position = Position.emptyPosition();

    if (retainDuplicates) {
      // TODO: we should implement support for retaining duplicates
      // I suspect this is a pretty niche use case, so this can wait for later
      // as it's only used to ensure the result of stream-stream joins include
      // duplicate results for the joins if there are duplicate keys in the source
      // with the same timestamp
      LOG.warn("ResponsiveWindowStore does not fully support retaining duplicates");
    }
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
          "Use ResponsiveWindowStore#init(StateStoreContext, StateStore) instead."
      );
    }
  }

  @Override
  public void init(final StateStoreContext context, final StateStore root) {
    try {
      LOG.info("Initializing state store {}", name);

      this.context = asInternalProcessorContext(context);

      final ResponsiveConfig config = new ResponsiveConfig(context.appConfigs());

      partition = context.taskId().partition();

      final SharedClients sharedClients = new SharedClients(context.appConfigs());
      client = sharedClients.cassandraClient;

      final RemoteMonitor monitor = client.awaitTable(name.cassandraName(), sharedClients.executor);
      client.createWindowedDataTable(name.cassandraName());
      monitor.await(Duration.ofSeconds(60));
      LOG.info("Remote table {} is available for querying.", name.cassandraName());


      client.prepareWindowedStatements(name.cassandraName());
      registry = InternalConfigs.loadStoreRegistry(context.appConfigs());
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
          new Plugin(this::withinRetention),
          StoreUtil.shouldTruncateChangelog(
              topicPartition.topic(),
              sharedClients.admin,
              context.appConfigs()
          ),
          FlushTriggers.fromConfig(config),
          partitioner
      );
      buffer.init();

      final long offset = buffer.offset();
      registration = new ResponsiveStoreRegistration(
          name.kafkaName(),
          topicPartition,
          offset == -1 ? 0 : offset,
          buffer::flush
      );
      registry.registerStore(registration);

      open = true;

      context.register(root, buffer);
    } catch (InterruptedException | TimeoutException e) {
      throw new ProcessorStateException("Failed to initialize store.", e);
    }
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
  public boolean isOpen() {
    return open;
  }

  private Supplier asRecordCollector(final StateStoreContext context) {
    return ((RecordCollector.Supplier) context);
  }

  @Override
  public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
    observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);

    final Stamped<Bytes> wKey = new Stamped<>(key, windowStartTimestamp);

    buffer.put(wKey, value);
    StoreQueryUtils.updatePosition(position, context);
  }

  @Override
  public byte[] fetch(final Bytes key, final long time) {
    final Result<Stamped<Bytes>> localResult = buffer.get(new Stamped<>(key, time));
    if (localResult != null)  {
      return localResult.isTombstone ? null : localResult.value;
    }

    final KeyValueIterator<Stamped<Bytes>, byte[]> remoteResult = client.fetch(
        name.cassandraName(),
        partitioner.partition(partition, key),
        key,
        time,
        time + 1
    );

    if (!remoteResult.hasNext()) {
      return null;
    }

    return remoteResult.next().value;
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final long start = Math.max(observedStreamTime - retentionPeriod, timeFrom);
    final Stamped<Bytes> from = new Stamped<>(key, start);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(partition, key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.range(from, to),
            client.fetch(name.cassandraName(), subPartition, key, start, timeTo),
            ResponsiveWindowStore::compareKeys
        )
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final long start = Math.max(observedStreamTime - retentionPeriod, timeFrom);
    final Stamped<Bytes> from = new Stamped<>(key, start);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(partition, key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.backRange(from, to),
            client.backFetch(name.cassandraName(), subPartition, key, start, timeTo),
            ResponsiveWindowStore::compareKeys
        )
    );
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(
      final Bytes keyFrom,
      final Bytes keyTo,
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  @Override
  public Position getPosition() {
    return position;
  }

  private boolean withinRetention(final Stamped<Bytes> key) {
    return key.stamp > observedStreamTime - retentionPeriod;
  }

  private static int compareKeys(final Stamped<Bytes> o1, final Stamped<Bytes> o2) {
    final int key = o1.key.compareTo(o2.key);
    if (key != 0) {
      return key;
    }

    return Long.compare(o1.stamp, o2.stamp);
  }

  private static class Plugin implements BufferPlugin<Stamped<Bytes>> {

    private final Predicate<Stamped<Bytes>> retain;

    public Plugin(final Predicate<Stamped<Bytes>> retain) {
      this.retain = retain;
    }

    @Override
    public Stamped<Bytes> keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
      final byte[] key = record.key();
      final int size = key.length - TIMESTAMP_SIZE;

      final ByteBuffer buffer = ByteBuffer.wrap(key);
      final long startTs = buffer.getLong(size);
      final Bytes kBytes = Bytes.wrap(Arrays.copyOfRange(key, 0, size));

      return new Stamped<>(kBytes, startTs);
    }

    @Override
    public BoundStatement insertData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Stamped<Bytes> key,
        final byte[] value
    ) {
      return client.insertWindowed(tableName, partition, key.key, key.stamp, value);
    }

    @Override
    public BoundStatement deleteData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Stamped<Bytes> key
    ) {
      throw new UnsupportedOperationException("Cannot delete windowed data using the delete API");
    }

    @Override
    public int compare(final Stamped<Bytes> o1, final Stamped<Bytes> o2) {
      return compareKeys(o1, o2);
    }

    @Override
    public boolean retain(final Stamped<Bytes> key) {
      return retain.test(key);
    }

    @Override
    public Bytes bytes(final Stamped<Bytes> key) {
      return key.key;
    }
  }
}
