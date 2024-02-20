/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.stores;

import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadSessionClients;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadStoreRegistry;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.CassandraTableSpecFactory;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.WindowedKeySpec;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.StoreUtil;
import dev.responsive.kafka.internal.utils.TableName;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class SegmentedOperations implements WindowOperations {

  @SuppressWarnings("rawtypes")
  private final InternalProcessorContext context;
  private final ResponsiveWindowParams params;
  private final RemoteWindowedTable<?> table;
  private final CommitBuffer<WindowedKey, ?> buffer;
  private final TopicPartition changelog;

  private final ResponsiveStoreRegistry storeRegistry;
  private final ResponsiveStoreRegistration registration;
  private final ResponsiveRestoreListener restoreListener;

  private final long initialStreamTime;

  public static SegmentedOperations create(
      final TableName name,
      final StateStoreContext storeContext,
      final ResponsiveWindowParams params,
      final Map<String, Object> appConfigs,
      final ResponsiveConfig responsiveConfig,
      final Predicate<WindowedKey> withinRetention
  ) throws InterruptedException, TimeoutException {

    final var log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(SegmentedOperations.class);
    final var context = asInternalProcessorContext(storeContext);


    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );

    final WindowSegmentPartitioner partitioner = new WindowSegmentPartitioner(
        params.retentionPeriod(),
        StoreUtil.computeSegmentInterval(params.retentionPeriod(), params.numSegments())
    );

    final RemoteWindowedTable<?> table;
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        table = createCassandra(params, sessionClients, partitioner);
        break;
      case MONGO_DB:
        table = createMongo(params, sessionClients, partitioner);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + sessionClients.storageBackend());
    }

    final WindowFlushManager flushManager = table.init(changelog.partition());

    log.info("Remote table {} is available for querying.", name.tableName());

    final WindowedKeySpec keySpec = new WindowedKeySpec(withinRetention);
    final BatchFlusher<WindowedKey, ?> batchFlusher = new BatchFlusher<>(
        keySpec,
        changelog.partition(),
        flushManager
    );
    final CommitBuffer<WindowedKey, ?> buffer = CommitBuffer.from(
        batchFlusher,
        sessionClients,
        changelog,
        keySpec,
        params.truncateChangelog(),
        params.name(),
        responsiveConfig
    );
    final long restoreStartOffset = table.fetchOffset(changelog.partition());
    final var registration = new ResponsiveStoreRegistration(
        name.kafkaName(),
        changelog,
        restoreStartOffset == -1 ? 0 : restoreStartOffset,
        buffer::flush
    );
    storeRegistry.registerStore(registration);

    return new SegmentedOperations(
        context,
        params,
        table,
        buffer,
        changelog,
        storeRegistry,
        registration,
        sessionClients.restoreListener(),
        flushManager.streamTime()
    );
  }

  private static RemoteWindowedTable<?> createCassandra(
      final ResponsiveWindowParams params,
      final SessionClients clients,
      final WindowSegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    final CassandraClient client = clients.cassandraClient();

    final var spec = CassandraTableSpecFactory.fromWindowParams(params, partitioner);

    switch (params.schemaType()) {
      case WINDOW:
        return client.windowedFactory().create(spec, partitioner);
      case STREAM:
        throw new UnsupportedOperationException("Not yet implemented");
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  private static RemoteWindowedTable<?> createMongo(
      final ResponsiveWindowParams params,
      final SessionClients clients,
      final WindowSegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    final ResponsiveMongoClient client = clients.mongoClient();

    switch (params.schemaType()) {
      case WINDOW:
        return client.windowedTable(params.name().tableName(), partitioner);
      case STREAM:
        throw new UnsupportedOperationException("Not yet implemented");
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  @SuppressWarnings("rawtypes")
  public SegmentedOperations(
      final InternalProcessorContext context,
      final ResponsiveWindowParams params,
      final RemoteWindowedTable table,
      final CommitBuffer<WindowedKey, ?> buffer,
      final TopicPartition changelog,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveStoreRegistration registration,
      final ResponsiveRestoreListener restoreListener,
      final long initialStreamTime
  ) {
    this.context = context;
    this.params = params;
    this.table = table;
    this.buffer = buffer;
    this.changelog = changelog;
    this.storeRegistry = storeRegistry;
    this.registration = registration;
    this.restoreListener = restoreListener;
    this.initialStreamTime = initialStreamTime;
  }

  @Override
  public long initialStreamTime() {
    return initialStreamTime;
  }

  @Override
  public void put(final Bytes key, final byte[] value, final long windowStartTime) {
    buffer.put(new WindowedKey(key, windowStartTime), value, context.timestamp());
  }

  @Override
  public void delete(final Bytes key, final long windowStartTime) {
    buffer.tombstone(new WindowedKey(key, windowStartTime), context.timestamp());
  }

  @Override
  public byte[] fetch(final Bytes key, final long windowStartTime) {
    final WindowedKey windowedKey = new WindowedKey(key, windowStartTime);
    final Result<WindowedKey> localResult = buffer.get(windowedKey);
    if (localResult != null) {
      return localResult.isTombstone ? null : localResult.value;
    }

    return table.fetch(
        changelog.partition(),
        key,
        windowStartTime
    );
  }

  @Override
  public WindowStoreIterator<byte[]> fetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final WindowedKey from = new WindowedKey(key, timeFrom);
    final WindowedKey to = new WindowedKey(key, timeTo);

    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.range(from, to),
            table.fetch(changelog.partition(), key, timeFrom, timeTo)
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
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
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
  public WindowStoreIterator<byte[]> backwardFetch(
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    final WindowedKey from = new WindowedKey(key, timeFrom);
    final WindowedKey to = new WindowedKey(key, timeTo);

    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.backRange(from, to),
            table.backFetch(changelog.partition(), key, timeFrom, timeTo)
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
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(
      final long timeFrom,
      final long timeTo
  ) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() {
    // no need to flush the buffer here, will happen through the kafka client commit as usual
    buffer.close();
    restoreListener.onStoreClosed(changelog, params.name().kafkaName());
    storeRegistry.deregisterStore(registration);
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    buffer.restoreBatch(records);
  }

}
