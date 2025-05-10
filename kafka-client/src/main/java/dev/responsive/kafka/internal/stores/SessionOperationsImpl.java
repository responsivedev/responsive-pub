/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadSessionClients;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadStoreRegistry;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static dev.responsive.kafka.internal.utils.StoreUtil.streamThreadId;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveSessionParams;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.RemoteSessionTable;
import dev.responsive.kafka.internal.db.SessionFlushManager;
import dev.responsive.kafka.internal.db.SessionKeySpec;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.SessionKey;
import dev.responsive.kafka.internal.utils.StoreUtil;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collection;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

public class SessionOperationsImpl implements SessionOperations {

  private final Logger log;

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
        String.format("session-store [%s] ", name.kafkaName())
    ).logger(SessionOperationsImpl.class);
    final var context = asInternalProcessorContext(storeContext);

    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );

    final SessionSegmentPartitioner partitioner = new SessionSegmentPartitioner(
        params.retentionPeriod(),
        StoreUtil.computeSegmentInterval(params.retentionPeriod(), params.numSegments())
    );

    final RemoteSessionTable<?> table =
        createRemoteSessionTable(params, sessionClients, partitioner, responsiveConfig, log);

    final SessionFlushManager flushManager = table.init(changelog.partition());

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
        params.name(),
        false,
        responsiveConfig
    );
    final long restoreStartOffset = table.lastWrittenOffset(changelog.partition());
    final var registration = new ResponsiveStoreRegistration(
        name.kafkaName(),
        changelog,
        restoreStartOffset == NO_COMMITTED_OFFSET
            ? OptionalLong.empty()
            : OptionalLong.of(restoreStartOffset),
        buffer::flush,
        streamThreadId(),
        storeContext.taskId()
    );
    storeRegistry.registerStore(registration);

    return new SessionOperationsImpl(
        log,
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
      final SessionSegmentPartitioner partitioner
  ) throws InterruptedException, TimeoutException {
    throw new UnsupportedOperationException(
        "Only responsive.storage.backend=MONGO_DB currently supports session windows.");
  }

  private static RemoteSessionTable<?> createMongo(
      final ResponsiveSessionParams params,
      final SessionClients clients,
      final SessionSegmentPartitioner partitioner,
      final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    final ResponsiveMongoClient client = clients.mongoClient();

    switch (params.schemaType()) {
      case SESSION:
        return client.sessionTable(params.name().tableName(), partitioner, config);
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  private static RemoteSessionTable<?> createRemoteSessionTable(
      final ResponsiveSessionParams params,
      final SessionClients sessionClients,
      final SessionSegmentPartitioner partitioner,
      final ResponsiveConfig config,
      final Logger log
  ) throws InterruptedException, TimeoutException {
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        return createCassandra(params, sessionClients, partitioner);
      case MONGO_DB:
        return createMongo(params, sessionClients, partitioner, config);
      case NONE:
        log.error("Must configure a storage backend type using the config {}",
                  STORAGE_BACKEND_TYPE_CONFIG);
        throw new IllegalStateException("Responsive stores require a storage backend to be"
                                            + " configured, got 'NONE'");
      default:
        throw new IllegalStateException("Unexpected value: " + sessionClients.storageBackend());
    }
  }

  @SuppressWarnings("rawtypes")
  public SessionOperationsImpl(
      final Logger log,
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
    this.log = log;
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
    this.buffer.put(key, value, this.context.timestamp());
  }

  @Override
  public void delete(final SessionKey key) {
    this.buffer.tombstone(key, this.context.timestamp());
  }

  @Override
  public byte[] fetch(final SessionKey key) {
    final Result<SessionKey> localResult = this.buffer.get(key);
    if (localResult != null) {
      return localResult.isTombstone ? null : localResult.value;
    }

    final var remoteResult = this.table.fetch(
        this.changelog.partition(),
        key.key,
        key.sessionStartMs,
        key.sessionEndMs
    );
    return remoteResult;
  }

  @Override
  public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(
      final Bytes key,
      final long earliestSessionEnd,
      final long latestSessionEnd
  ) {
    final var localResults = this.buffer.all(kv -> {
      return kv.key.equals(key) && kv.sessionEndMs >= earliestSessionEnd
          && kv.sessionEndMs <= latestSessionEnd;
    });
    final var remoteResults = this.table.fetchAll(
        this.changelog.partition(),
        key,
        earliestSessionEnd,
        latestSessionEnd
    );

    final var combinedResults = new LocalRemoteKvIterator<>(localResults, remoteResults);
    final var keysOnly = Iterators.mapKeys(
        combinedResults,
        sessionKey -> {
          final var sessionWindow =
              new SessionWindow(sessionKey.sessionStartMs, sessionKey.sessionEndMs);
          return new Windowed<>(sessionKey.key, sessionWindow);
        }
    );
    return Iterators.filterKv(keysOnly, session -> {
      return session.window().end() >= earliestSessionEnd
          && session.window().end() <= latestSessionEnd;
    });
  }

  @Override
  public void close() {
    // no need to flush the buffer here, will happen through the kafka client commit as usual
    this.buffer.close();
    this.restoreListener.onStoreClosed(changelog, params.name().kafkaName());
    this.storeRegistry.deregisterStore(registration);
  }

  @Override
  public long restoreBatch(
      final Collection<ConsumerRecord<byte[], byte[]>> records,
      final long streamTimeMs
  ) {
    return buffer.restoreBatch(records, streamTimeMs);
  }

}
