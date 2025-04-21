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

import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.ClockType;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.TtlResolver;
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

  // kafka store names to track which stores we've created in RS3
  private final Map<String, UUID> createdStores = new ConcurrentHashMap<>();

  public RS3TableFactory(GrpcRS3Client.Connector connector) {
    this.connector = connector;
  }

  public RemoteKVTable<WalEntry> kvTable(
      final String storeName,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder,
      final Supplier<Integer> computeNumKafkaPartitions
  ) {
    final var rs3Client = connector.connect();

    final UUID storeId = createdStores.computeIfAbsent(storeName, n -> createStore(
        storeName, ttlResolver, computeNumKafkaPartitions.get(), rs3Client
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

  public static UUID createStore(
      final String storeName,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final int numKafkaPartitions,
      final RS3Client rs3Client
  ) {

    final Optional<Long> defaultTtl =
        ttlResolver.isPresent() && ttlResolver.get().defaultTtl().isFinite()
            ? Optional.of(ttlResolver.get().defaultTtl().duration().toMillis())
            : Optional.empty();

    final var options = new CreateStoreOptions(
        ttlResolver.isPresent() ? Optional.of(ClockType.WALL_CLOCK) : Optional.empty(),
        defaultTtl,
        Optional.empty()
    );

    final var result = rs3Client.createStore(storeName, numKafkaPartitions, options);
    LOG.info("Created store {} ({}) with {} logical shards and {} physical shards",
             storeName, result.storeId(), numKafkaPartitions, result.pssIds().size());

    return result.storeId();
  }

  public void close() {
  }
}
