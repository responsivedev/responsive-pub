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
import dev.responsive.kafka.internal.db.CassandraTableSpecFactory;
import dev.responsive.kafka.internal.db.RemoteWindowedTable;
import dev.responsive.kafka.internal.db.StampedKeySpec;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.Stamped;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
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
  private final StampedKeySpec keySpec;
  private final RemoteWindowedTable<?> table;
  private final CommitBuffer<Stamped<Bytes>, RemoteWindowedTable<?>> buffer;
  private final SubPartitioner partitioner;
  private final TopicPartition changelog;

  private final ResponsiveStoreRegistry storeRegistry;
  private final ResponsiveStoreRegistration registration;
  private final ResponsiveRestoreListener restoreListener;

  public static SegmentedOperations create(
      final TableName name,
      final StateStoreContext storeContext,
      final ResponsiveWindowParams params,
      final Predicate<Stamped<Bytes>> withinRetention
  ) throws InterruptedException, TimeoutException {

    final var log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(SegmentedOperations.class);
    final var context = asInternalProcessorContext(storeContext);

    // Save this so we don't have to rebuild the config map on every access
    final var appConfigs = storeContext.appConfigs();

    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(appConfigs);
    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );
    final var partitioner = config.getSegmentedSubPartitioner(
        sessionClients.admin(), 
        name, 
        changelog.topic()
    );

    final RemoteWindowedTable<?> table;
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        table = createCassandra(params, sessionClients);
        break;
      case MONGO_DB:
        throw new UnsupportedOperationException("Window stores are not yet compatible with Mongo");
      default:
        throw new IllegalStateException("Unexpected value: " + sessionClients.storageBackend());
    }

    log.info("Remote table {} is available for querying.", name.remoteName());

    final StampedKeySpec keySpec = new StampedKeySpec(withinRetention);
    final CommitBuffer<Stamped<Bytes>, RemoteWindowedTable<?>> buffer = CommitBuffer.from(
        sessionClients,
        changelog,
        table,
        keySpec,
        params.truncateChangelog(),
        params.name().kafkaName(),
        partitioner,
        config
    );
    buffer.init();

    final long offset = buffer.offset();
    final var registration = new ResponsiveStoreRegistration(
        name.kafkaName(),
        changelog,
        offset == -1 ? 0 : offset,
        buffer::flush
    );
    storeRegistry.registerStore(registration);

    return new SegmentedOperations(
        params,
        keySpec,
        table,
        buffer,
        partitioner,
        changelog,
        context,
        storeRegistry,
        registration,
        sessionClients.restoreListener()
    );
  }

  private static RemoteWindowedTable<?> createCassandra(
      final ResponsiveWindowParams params,
      final SessionClients clients
  ) throws InterruptedException, TimeoutException {
    final var client = clients.cassandraClient();
    final var spec = CassandraTableSpecFactory.fromWindowParams(params);
    switch (params.schemaType()) {
      case WINDOW:
        return client.windowedFactory().create(spec);
      case STREAM:
        throw new UnsupportedOperationException("Not yet implemented");
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  @SuppressWarnings("rawtypes")
  public SegmentedOperations(
      final ResponsiveWindowParams params,
      final StampedKeySpec keySpec,
      final RemoteWindowedTable table,
      final CommitBuffer<Stamped<Bytes>, RemoteWindowedTable<?>> buffer,
      final SubPartitioner partitioner,
      final TopicPartition changelog,
      final InternalProcessorContext context,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveStoreRegistration registration,
      final ResponsiveRestoreListener restoreListener
  ) {
    this.params = params;
    this.keySpec = keySpec;
    this.table = table;
    this.buffer = buffer;
    this.partitioner = partitioner;
    this.changelog = changelog;
    this.context = context;
    this.storeRegistry = storeRegistry;
    this.registration = registration;
    this.restoreListener = restoreListener;
  }
  
  @Override
  public void put(final Stamped<Bytes> windowedKey, final byte[] value) {
    buffer.put(windowedKey, value, context.timestamp());
  }

  @Override
  public void delete(final Stamped<Bytes> windowedKey) {
    buffer.tombstone(windowedKey, context.timestamp());
  }

  @Override
  public byte[] fetch(final Bytes key, final long windowStartTime) {
    final Result<Stamped<Bytes>> localResult = buffer.get(new Stamped<>(key, windowStartTime));
    if (localResult != null)  {
      return localResult.isTombstone ? null : localResult.value;
    }

    return table.fetch(
        partitioner.partition(changelog.partition(), key),
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
    final Stamped<Bytes> from = new Stamped<>(key, timeFrom);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(changelog.partition(), key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.range(from, to),
            table.fetch(subPartition, key, timeFrom, timeTo),
            keySpec
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
    final Stamped<Bytes> from = new Stamped<>(key, timeFrom);
    final Stamped<Bytes> to = new Stamped<>(key, timeTo);

    final int subPartition = partitioner.partition(changelog.partition(), key);
    return Iterators.windowed(
        new LocalRemoteKvIterator<>(
            buffer.backRange(from, to),
            table.backFetch(subPartition, key, timeFrom, timeTo),
            keySpec
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
