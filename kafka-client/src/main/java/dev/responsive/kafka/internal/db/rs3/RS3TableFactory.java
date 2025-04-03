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

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreOptions.ClockType;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RS3TableFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RS3TableFactory.class);
  private final GrpcRS3Client.Connector connector;

  // kafka store names to track which stores we've created in RS3
  private final Set<String> createdStores = new HashSet<>();

  public RS3TableFactory(GrpcRS3Client.Connector connector) {
    this.connector = connector;
  }

  public RemoteKVTable<WalEntry> kvTable(
      final String storeName,
      final ResponsiveConfig config,
      final Optional<TtlResolver<?, ?>> ttlResolver,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder,
      final Supplier<Integer> computeNumKafkaPartitions
  ) {
    final Map<String, String> storeIdMapping = config.getMap(
        ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG);
    final String storeIdHex = storeIdMapping.get(storeName);
    if (storeIdHex == null) {
      throw new ConfigException("Failed to find store ID mapping for table " + storeName);
    }

    final UUID storeId = UUID.fromString(storeIdHex);

    final var rs3Client = connector.connect();

    if (!createdStores.contains(storeName)) {
      final int kafkaPartitions = computeNumKafkaPartitions.get();

      final Optional<Long> defaultTtl = ttlResolver.isPresent() && ttlResolver.get().defaultTtl().isFinite()
          ? Optional.of(ttlResolver.get().defaultTtl().duration().toMillis())
          : Optional.empty();

      final var options = new CreateStoreOptions(
          ttlResolver.isPresent() ? Optional.of(ClockType.WALL_CLOCK) : Optional.empty(),
          defaultTtl,
          Optional.empty()
      );
      
      final var resultErr = rs3Client.createStore(storeId, kafkaPartitions, options);
      if (resultErr.isPresent()) {
        LOG.error("RS3 store creation failed with error code '{}' on store {}",
                  resultErr.get().name(), storeName);
        throw new StreamsException("Failed to create remote store " + storeName);
      }

      createdStores.add(storeName);
    }

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

  public void close() {
  }
}
