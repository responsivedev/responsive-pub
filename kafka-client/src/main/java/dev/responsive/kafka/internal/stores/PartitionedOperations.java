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
import dev.responsive.kafka.api.config.ResponsiveMode;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.config.ConfigUtils;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.db.FlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteTableSpecFactory;
import dev.responsive.kafka.internal.db.inmemory.InMemoryKVTable;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.TableName;
import dev.responsive.kafka.internal.utils.Utils;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

public class PartitionedOperations implements KeyValueOperations {

  private final Logger log;

  @SuppressWarnings("rawtypes")
  private final InternalProcessorContext context;
  private final ResponsiveKeyValueParams params;
  private final RemoteKVTable<?> table;
  private final CommitBuffer<Bytes, ?> buffer;
  private final TopicPartition changelog;

  private final ResponsiveStoreRegistry storeRegistry;
  private final ResponsiveStoreRegistration registration;
  private final ResponsiveRestoreListener restoreListener;
  private final boolean migrationMode;
  private final long startingTimestamp;

  private long streamTimeMs = -1L;

  public static PartitionedOperations create(
      final TableName name,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final StateStoreContext storeContext,
      final ResponsiveKeyValueParams params
  ) throws InterruptedException, TimeoutException {

    final var log = new LogContext(
        String.format("store [%s] ", name.kafkaName())
    ).logger(PartitionedOperations.class);
    final var context = asInternalProcessorContext(storeContext);

    // Save this so we don't have to rebuild the config map on every access
    final var appConfigs = storeContext.appConfigs();

    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(appConfigs);
    final SessionClients sessionClients = loadSessionClients(appConfigs);
    final ResponsiveStoreRegistry storeRegistry = loadStoreRegistry(appConfigs);
    final ResponsiveMetrics responsiveMetrics = loadMetrics(appConfigs);

    final TopicPartition changelog = new TopicPartition(
        changelogFor(storeContext, name.kafkaName(), false),
        context.taskId().partition()
    );

    final RemoteKVTable<?> table;
    switch (sessionClients.storageBackend()) {
      case CASSANDRA:
        table = createCassandra(params, config, sessionClients, changelog.topic(), ttlResolver);
        break;
      case MONGO_DB:
        table = createMongo(params, sessionClients, ttlResolver, config);
        break;
      case IN_MEMORY:
        table = createInMemory(params, ttlResolver, config);
        break;
      case RS3:
        final ResponsiveMetrics.MetricScopeBuilder scopeBuilder;
        scopeBuilder = responsiveMetrics.storeLevelMetricScopeBuilder(
            Utils.extractThreadIdFromThreadName(Thread.currentThread().getName()),
            changelog,
            params.name().tableName()
        );
        table = createRS3(
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

    final FlushManager<Bytes, ?> flushManager = table.init(changelog.partition());

    log.info("Remote table {} is available for querying.", name.tableName());

    final BytesKeySpec keySpec = new BytesKeySpec();
    final BatchFlusher<Bytes, ?> batchFlusher = new BatchFlusher<>(
        keySpec,
        changelog.partition(),
        flushManager
    );

    // these objects need to be cleaned up once they're created. If this method fails
    // then we should make sure we do the cleanup here.
    CommitBuffer<Bytes, ?> buffer = null;
    ResponsiveStoreRegistration registration = null;
    try {
      buffer = CommitBuffer.from(
          batchFlusher,
          sessionClients,
          changelog,
          keySpec,
          params.name(),
          false,
          config
      );

      final long restoreStartOffset = table.lastWrittenOffset(changelog.partition());
      final CommitBuffer<Bytes, ?> initializedBuffer = buffer;
      registration = new ResponsiveStoreRegistration(
          name.kafkaName(),
          changelog,
          restoreStartOffset == NO_COMMITTED_OFFSET
              ? OptionalLong.empty()
              : OptionalLong.of(restoreStartOffset),
          new ResponsiveStoreRegistration.StoreCallbacks() {
            @Override
            public void notifyCommit(long committedOffset) {
              initializedBuffer.flush(committedOffset);
            }

            @Override
            public byte[] checkpoint() {
              return table.checkpoint();
            }
          },
          streamThreadId()
      );
      storeRegistry.registerStore(registration);

      final boolean migrationMode = ConfigUtils.responsiveMode(config) == ResponsiveMode.MIGRATE;
      long startTimeMs = -1;
      if (migrationMode && params.ttlProvider().isPresent()) {
        if (!params.ttlProvider().get().hasDefaultOnly()) {
          throw new UnsupportedOperationException("Row-level ttl overrides are not yet supported "
                                                      + "with migration mode");
        }
        startTimeMs =
            System.currentTimeMillis() - params.ttlProvider().get().defaultTtl().toMillis();
      }

      return new PartitionedOperations(
          log,
          context,
          params,
          table,
          buffer,
          changelog,
          storeRegistry,
          registration,
          sessionClients.restoreListener(),
          migrationMode,
          startTimeMs
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

  private static RemoteKVTable<?> createInMemory(
      final ResponsiveKeyValueParams params,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveConfig config
  ) {
    if (ttlResolver.isPresent() && !ttlResolver.get().hasDefaultOnly()) {
      throw new UnsupportedOperationException("Row-level ttl is not yet supported "
                                                  + "for in-memory stores");
    }
    return new InMemoryKVTable(params.name().tableName(), ttlResolver);
  }

  private static RemoteKVTable<?> createCassandra(
      final ResponsiveKeyValueParams params,
      final ResponsiveConfig config,
      final SessionClients sessionClients,
      final String changelogTopicName,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) throws InterruptedException, TimeoutException {

    final int numChangelogPartitions =
        numPartitionsForKafkaTopic(sessionClients.admin(), changelogTopicName);

    // TODO(agavra): write the actual remote partition count into cassandra
    final OptionalInt actualRemoteCount = OptionalInt.empty();

    final TablePartitioner<Bytes, Integer> partitioner =
        params.schemaType() == SchemaTypes.KVSchema.FACT
        ? TablePartitioner.defaultPartitioner()
        : SubPartitioner.create(
            actualRemoteCount,
            numChangelogPartitions,
            params.name().tableName(),
            config,
            changelogTopicName
        );
    final var client = sessionClients.cassandraClient();
    final var spec = RemoteTableSpecFactory.fromKVParams(params, partitioner, ttlResolver, config);
    switch (params.schemaType()) {
      case KEY_VALUE:
        return client.kvFactory().create(spec);
      case FACT:
        return client.factFactory().create(spec);
      default:
        throw new IllegalArgumentException("Unexpected schema type " + params.schemaType());
    }
  }

  private static RemoteKVTable<?> createMongo(
      final ResponsiveKeyValueParams params,
      final SessionClients sessionClients,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveConfig config
  ) throws InterruptedException, TimeoutException {
    return sessionClients.mongoClient().kvTable(params.name().tableName(), ttlResolver, config);
  }

  private static RemoteKVTable<?> createRS3(
      final ResponsiveKeyValueParams params,
      final SessionClients sessionClients,
      final String changelogTopicName,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    return sessionClients.rs3TableFactory().kvTable(
        params.name().tableName(),
        ttlResolver,
        responsiveMetrics,
        scopeBuilder,
        () -> numPartitionsForKafkaTopic(sessionClients.admin(), changelogTopicName),
        params.schemaType()
    );
  }

  @SuppressWarnings("rawtypes")
  public PartitionedOperations(
      final Logger log,
      final InternalProcessorContext context,
      final ResponsiveKeyValueParams params,
      final RemoteKVTable<?> table,
      final CommitBuffer<Bytes, ?> buffer,
      final TopicPartition changelog,
      final ResponsiveStoreRegistry storeRegistry,
      final ResponsiveStoreRegistration registration,
      final ResponsiveRestoreListener restoreListener,
      final boolean migrationMode,
      final long startingTimestamp
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
    this.migrationMode = migrationMode;
    this.startingTimestamp = startingTimestamp;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    final long currentRecordTimestamp = currentRecordTimestamp();
    if (migratingAndTimestampTooEarly(currentRecordTimestamp)) {
      // we are bootstrapping a store. Only apply the write if the timestamp
      // is fresher than the starting timestamp
      return;
    }
    if (streamTimeMs < currentRecordTimestamp) {
      streamTimeMs = currentRecordTimestamp;
    }

    buffer.put(key, value, currentRecordTimestamp);
  }

  @Override
  public byte[] delete(final Bytes key) {
    final long currentRecordTimestamp = currentRecordTimestamp();

    if (streamTimeMs < currentRecordTimestamp) {
      streamTimeMs = currentRecordTimestamp;
    }

    // single writer prevents races (see putIfAbsent)
    final byte[] old = get(key);
    buffer.tombstone(key, currentRecordTimestamp());

    return old;
  }

  @Override
  public byte[] get(final Bytes key) {
    final long currentRecordTimestamp = currentRecordTimestamp();

    // streamTime is used for ttl so we want to advance it before/during the get, not only on put
    if (streamTimeMs < currentRecordTimestamp) {
      streamTimeMs = currentRecordTimestamp;
    }

    if (migrationMode) {
      // we don't want to issue gets in migration mode since
      // we're just reading from the changelog. the problem is
      // that materialized tables issue get() on every put() to
      // send the "undo" data downstream -- we intercept all gets
      // and just return null
      return null;
    }

    // try the buffer first, it acts as a local cache
    // but this is also necessary for correctness as
    // it is possible that the data is either uncommitted
    // or not yet pushed to the remote store
    // TODO: strictly speaking we should be filtering the result by timestamp. However
    //  the lifetime of entries in the CommitBuffer is generally likely to be much
    //  smaller than the ttl, so it's probably ok to skip this check for now
    final Result<Bytes> result = buffer.get(key);
    if (result != null) {
      return result.isTombstone ? null : result.value;
    }

    return table.get(
        changelog.partition(),
        key,
        streamTimeMs
    );
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
    if (from == null && to == null) {
      return all();
    } else if (from == null || to == null) {
      log.error("Unable to serve range query with undefined bounds. Found {}=null",
                from == null ? "from" : "to");
      throw new UnsupportedOperationException(
          "Open-ended range queries are not yet supported, please pass in non-null values for the "
              + "lower and upper bounds or else use all() and filter the results. If your use case "
              + "requires the ability to issue range queries with an undefined upper/lower bound, "
              + "please reach out to us about supporting this feature.");
    }

    return new LocalRemoteKvIterator<>(
        buffer.range(from, to),
        table.range(changelog.partition(), from, to, streamTimeMs)
    );
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
    // TODO: add a reverseRange API to RemoteKVTable (or add an iteration order param to #range)
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefix(
      final P prefix,
      final PS prefixKeySerializer
  ) {
    return table.prefix(prefix, prefixKeySerializer, changelog.partition(), streamTimeMs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return new LocalRemoteKvIterator<>(
        buffer.all(),
        table.all(changelog.partition(), streamTimeMs)
    );
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    // TODO: add a reverseAll API to RemoteKVTable (or add an iteration order param to #all)
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public long approximateNumEntries() {
    return table.approximateNumEntries(changelog.partition());
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
    streamTimeMs = Math.max(
        streamTimeMs,
        buffer.restoreBatch(records, streamTimeMs)
    );
  }

  private long currentRecordTimestamp() {
    final InjectedStoreArgs injectedStoreArgs = registration.injectedStoreArgs();
    final Optional<Supplier<Long>> injectedClock = injectedStoreArgs.recordTimestampClock();
    if (injectedClock.isPresent()) {
      return injectedClock.get().get();
    }
    return context.timestamp();
  }

  private boolean migratingAndTimestampTooEarly(final long currentRecordTimestamp) {
    if (!migrationMode) {
      return false;
    }
    if (startingTimestamp > 0) {
      // we are bootstrapping a store. Only apply the write if the timestamp
      // is fresher than the starting timestamp
      return currentRecordTimestamp < startingTimestamp;
    }
    return false;
  }
}
