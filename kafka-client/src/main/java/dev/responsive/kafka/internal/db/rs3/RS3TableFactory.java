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

package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.api.config.RS3ConfigSetter;
import dev.responsive.kafka.api.config.RS3StoreParams;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.ClockType;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.SlateDbStorageOptions;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RS3TableFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RS3TableFactory.class);

  private final GrpcRS3Client.Connector connector;
  private final RS3ConfigSetter configSetter;

  // kafka store names to track which stores we've created in RS3
  private final Map<String, UUID> createdStores = new ConcurrentHashMap<>();

  public RS3TableFactory(
      final GrpcRS3Client.Connector connector,
      final RS3ConfigSetter configSetter
  ) {
    this.connector = connector;
    this.configSetter = configSetter;
  }

  public RemoteKVTable<WalEntry> kvTable(
      final String storeName,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder,
      final Supplier<Integer> computeNumKafkaPartitions,
      final KVSchema schema
  ) {
    final Optional<Duration> defaultTtl = ttlResolver.isPresent()
        && ttlResolver.get().defaultTtl().isFinite()
        ? Optional.of(ttlResolver.get().defaultTtl().duration())
        : Optional.empty();
    final Optional<ClockType> clockType = ttlResolver.isPresent()
        ? Optional.of(ClockType.WALL_CLOCK)
        : Optional.empty();

    final RS3StoreParams params = configSetter.keyValueStoreConfig(storeName, schema);
    final var storageOptions = new SlateDbStorageOptions(params.filterBitsPerKey());

    final var rs3Client = connector.connect();

    final UUID storeId = createdStores.computeIfAbsent(
        storeName,
        n -> createStore(
            storeName,
            CreateStoreTypes.StoreType.BASIC,
            clockType,
            defaultTtl,
            Optional.of(storageOptions),
            computeNumKafkaPartitions.get(),
            rs3Client
        ));

    final PssPartitioner pssPartitioner = new PssDirectPartitioner();
    return new RS3KVTable(
        storeName,
        storeId,
        rs3Client,
        pssPartitioner,
        responsiveMetrics,
        scopeBuilder
    );
  }

  public RemoteWindowTable<WalEntry> windowTable(
      final String storeName,
      final Duration defaultTtl,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder,
      final Supplier<Integer> computeNumKafkaPartitions,
      final WindowSchema schema
  ) {
    final var rs3Client = connector.connect();

    final RS3StoreParams params = configSetter.windowStoreConfig(storeName, schema);
    final var storageOptions = new SlateDbStorageOptions(params.filterBitsPerKey());

    final UUID storeId = createdStores.computeIfAbsent(storeName, n -> createStore(
        storeName,
        CreateStoreTypes.StoreType.WINDOW,
        Optional.of(ClockType.STREAM_TIME),
        Optional.of(defaultTtl),
        Optional.of(storageOptions),
        computeNumKafkaPartitions.get(),
        rs3Client
    ));

    final var pssPartitioner = new PssDirectPartitioner();
    return new RS3WindowTable(
        storeName,
        storeId,
        rs3Client,
        pssPartitioner,
        responsiveMetrics,
        scopeBuilder
    );
  }

  private static UUID createStore(
      final String storeName,
      final CreateStoreTypes.StoreType storeType,
      final Optional<ClockType> clockType,
      final Optional<Duration> defaultTtl,
      final Optional<SlateDbStorageOptions> storageOptions,
      final int numKafkaPartitions,
      final RS3Client rs3Client
  ) {
    final var options = new CreateStoreOptions(
        numKafkaPartitions,
        storeType,
        clockType,
        defaultTtl.map(Duration::toMillis),
        storageOptions
    );

    final var result = rs3Client.createStore(storeName, options);
    LOG.info("Created store {} ({}) with {} logical shards and {} physical shards",
             storeName, result.storeId(), numKafkaPartitions, result.pssIds().size());

    return result.storeId();
  }

  public void close() {
  }
}
