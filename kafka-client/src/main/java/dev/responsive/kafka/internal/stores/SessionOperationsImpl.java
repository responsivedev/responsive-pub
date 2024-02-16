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
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.RemoteSessionTable;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.SessionKeySpec;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.SessionKey;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class SessionOperationsImpl implements SessionOperations {

  @SuppressWarnings("rawtypes")
  private final InternalProcessorContext context;
  private final ResponsiveSessionParams params;
  private final RemoteSessionTable<?> table;
  private final CommitBuffer<SessionKey, ?> buffer;
  private final TopicPartition changelog;

  private final ResponsiveStoreRegistry storeRegistry;
  private final ResponsiveStoreRegistration registration;
  private final ResponsiveRestoreListener restoreListener;

  private final long initialStreamTime;

  public static SessionOperationsImpl create(
      final TableName name,
      final StateStoreContext storeContext,
      final ResponsiveSessionParams params,
      final Map<String, Object> appConfigs,
      final ResponsiveConfig responsiveConfig,
      final Predicate<SessionKey> withinRetention
  ) throws InterruptedException, TimeoutException {

    final var log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(SessionOperationsImpl.class);
    final var context = asInternalProcessorContext(storeContext);


    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );

    final SegmentPartitioner partitioner = SegmentPartitioner.create(params);

    final RemoteSessionTable<?> table =
        createRemoteSessionTable(params, sessionClients, partitioner);

    final WindowFlushManager flushManager = table.init(changelog.partition());

    log.info("Remote table {} is available for querying.", name.tableName());

    final SessionKeySpec keySpec = new SessionKeySpec(withinRetention);
    final BatchFlusher<SessionKey, ?> batchFlusher = new BatchFlusher<>(
        keySpec,
        changelog.partition(),
        flushManager
    );
    final CommitBuffer<SessionKey, ?> buffer = CommitBuffer.from(
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

    return new SessionOperationsImpl(
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

  private static RemoteSessionTable<?> createCassandra(
      final ResponsiveSessionParams params,
      final SessionClients clients,
      final SegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private static RemoteSessionTable<?> createMongo(
      final ResponsiveSessionParams params,
      final SessionClients clients,
      final SegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    final ResponsiveMongoClient client = clients.mongoClient();

    switch (params.schemaType()) {
      case SESSION:
        return client.sessionTable(params.name().tableName(), partitioner);
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  private static RemoteSessionTable<?> createRemoteSessionTable(
      final ResponsiveSessionParams params,
      final SessionClients sessionClients,
      final SegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        return createCassandra(params, sessionClients, partitioner);
      case MONGO_DB:
        return createMongo(params, sessionClients, partitioner);
      default:
        throw new IllegalStateException("Unexpected value: " + sessionClients.storageBackend());
    }
  }

  @SuppressWarnings("rawtypes")
  public SessionOperationsImpl(
      final InternalProcessorContext context,
      final ResponsiveSessionParams params,
      final RemoteSessionTable table,
      final CommitBuffer<SessionKey, ?> buffer,
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
  public void put(
      SessionKey key,
      final byte[] value
  ) {
    buffer.put(key, value, context.timestamp());
  }

  @Override
  public void delete(final SessionKey key) {
    buffer.tombstone(key, context.timestamp());
  }

  @Override
  public byte[] fetch(final SessionKey key) {
    final Result<SessionKey> localResult = buffer.get(key);
    if (localResult != null) {
      return localResult.isTombstone ? null : localResult.value;
    }

    return table.fetch(
        changelog.partition(),
        key.key,
        key.sessionStartMs,
        key.sessionEndMs
    );
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
