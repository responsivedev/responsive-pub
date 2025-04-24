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
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadMetrics;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadSessionClients;
import static dev.responsive.kafka.internal.config.InternalSessionConfigs.loadStoreRegistry;
import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
import static dev.responsive.kafka.internal.utils.StoreUtil.numPartitionsForKafkaTopic;
import static dev.responsive.kafka.internal.utils.StoreUtil.streamThreadId;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.changelogFor;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.RemoteTableSpecFactory;
import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.WindowedKeySpec;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.StoreUtil;
import dev.responsive.kafka.internal.utils.TableName;
import dev.responsive.kafka.internal.utils.Utils;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import org.slf4j.Logger;

public class RemoteWindowOperations implements WindowOperations {

  private final Logger log;

  @SuppressWarnings("rawtypes")
  private final InternalProcessorContext context;
  private final ResponsiveWindowParams params;
  private final RemoteWindowTable<?> table;
  private final CommitBuffer<WindowedKey, ?> buffer;
  private final TopicPartition changelog;

  private final ResponsiveStoreRegistry storeRegistry;
  private final ResponsiveStoreRegistration registration;
  private final ResponsiveRestoreListener restoreListener;

  private final long initialStreamTime;

  public static RemoteWindowOperations create(
      final TableName name,
      final StateStoreContext storeContext,
      final ResponsiveWindowParams params,
      final Map<String, Object> appConfigs,
      final ResponsiveConfig responsiveConfig,
      final Predicate<WindowedKey> withinRetention,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) throws InterruptedException, TimeoutException {

    final var log = new LogContext(
        String.format("window-store [%s] ", name.kafkaName())
    ).logger(RemoteWindowOperations.class);
    final var context = asInternalProcessorContext(storeContext);

    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );

    final WindowSegmentPartitioner partitioner = new WindowSegmentPartitioner(
        params.retentionPeriod(),
        StoreUtil.computeSegmentInterval(params.retentionPeriod(), params.numSegments()),
        params.retainDuplicates()
    );

    final RemoteWindowTable<?> table;
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        table = createCassandra(params, sessionClients, partitioner, responsiveConfig);
        break;
      case MONGO_DB:
        table = createMongo(params, sessionClients, partitioner, responsiveConfig);
        break;
      case RS3:
        final var responsiveMetrics = loadMetrics(appConfigs);
        final var scopeBuilder = responsiveMetrics.storeLevelMetricScopeBuilder(
            Utils.extractThreadIdFromThreadName(Thread.currentThread().getName()),
            changelog,
            params.name().tableName()
        );

        table = createRs3(
            params,
            sessionClients,
            changelog.topic(),
            ttlResolver,
            responsiveMetrics,
            scopeBuilder
        );
        break;
      case NONE:
        log.error("Must configure a storage backend type using the config {}",
                  STORAGE_BACKEND_TYPE_CONFIG);
        throw new IllegalStateException("Responsive stores require a storage backend to be"
                                            + " configured, got 'NONE'");
      default:
        throw new IllegalStateException("Unrecognized value: " + sessionClients.storageBackend());
    }

    final WindowFlushManager<?> flushManager = table.init(changelog.partition());

    log.info("Remote table {} is available for querying.", name.tableName());

    final WindowedKeySpec keySpec = new WindowedKeySpec(withinRetention);
    final BatchFlusher<WindowedKey, ?> batchFlusher = new BatchFlusher<>(
        keySpec,
        changelog.partition(),
        flushManager
    );

    // these objects need to be cleaned up once they're created. If this method fails
    // then we should make sure we do the cleanup here.
    CommitBuffer<WindowedKey, ?> buffer = null;
    ResponsiveStoreRegistration registration = null;
    try {
      buffer = CommitBuffer.from(
          batchFlusher,
          sessionClients,
          changelog,
          keySpec,
          params.name(),
          params.retainDuplicates(),
          responsiveConfig
      );
      final long restoreStartOffset = table.lastWrittenOffset(changelog.partition());
      registration = new ResponsiveStoreRegistration(
          name.kafkaName(),
          changelog,
          restoreStartOffset == NO_COMMITTED_OFFSET
              ? OptionalLong.empty()
              : OptionalLong.of(restoreStartOffset),
          buffer::flush,
          streamThreadId()
      );
      storeRegistry.registerStore(registration);

      return new RemoteWindowOperations(
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
    } catch (final RuntimeException e) {
      if (buffer != null) {
        buffer.close();
      }
      if (registration != null) {
        storeRegistry.deregisterStore(registration);
      }
      throw e;
    }
  }

  private static RemoteWindowTable<?> createCassandra(
      final ResponsiveWindowParams params,
      final SessionClients clients,
      final WindowSegmentPartitioner partitioner,
      final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    final CassandraClient client = clients.cassandraClient();

    final var spec = RemoteTableSpecFactory.fromWindowParams(params, partitioner, config);

    switch (params.schemaType()) {
      case WINDOW:
        return client.windowedFactory().create(spec, partitioner);
      case STREAM:
        throw new UnsupportedOperationException("Not yet implemented");
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  private static RemoteWindowTable<?> createMongo(
      final ResponsiveWindowParams params,
      final SessionClients clients,
      final WindowSegmentPartitioner partitioner,
      final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    final ResponsiveMongoClient client = clients.mongoClient();

    switch (params.schemaType()) {
      case WINDOW:
        return client.windowedTable(params.name().tableName(), partitioner, config);
      case STREAM:
        throw new UnsupportedOperationException("Not yet implemented");
      default:
        throw new IllegalArgumentException(params.schemaType().name());
    }
  }

  private static RemoteWindowTable<?> createRs3(
      final ResponsiveWindowParams params,
      final SessionClients sessionClients,
      final String changelogTopicName,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    if (params.schemaType() == SchemaTypes.WindowSchema.STREAM || params.retainDuplicates()) {
      throw new UnsupportedOperationException("Duplicate retention is not yet supported in RS3");
    }

    return sessionClients.rs3TableFactory().windowTable(
        params.name().tableName(),
        ttlResolver,
        responsiveMetrics,
        scopeBuilder,
        () -> numPartitionsForKafkaTopic(sessionClients.admin(), changelogTopicName)
    );
  }

  @SuppressWarnings("rawtypes")
  public RemoteWindowOperations(
      final Logger log,
      final InternalProcessorContext context,
      final ResponsiveWindowParams params,
      final RemoteWindowTable table,
      final CommitBuffer<WindowedKey, ?> buffer,
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
            table.fetch(changelog.partition(), key, timeFrom, timeTo),
            params.retainDuplicates()
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
            table.backFetch(changelog.partition(), key, timeFrom, timeTo),
            params.retainDuplicates()
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
  public long restoreBatch(
      final Collection<ConsumerRecord<byte[], byte[]>> records,
      final long streamTimeMs
  ) {
    return buffer.restoreBatch(records, streamTimeMs);
  }

}
