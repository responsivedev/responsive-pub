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
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigException;

public class RS3TableFactory {
  private final GrpcRS3Client.Connector connector;

  public RS3TableFactory(
      GrpcRS3Client.Connector connector
  ) {
    this.connector = connector;
  }

  public RemoteKVTable<WalEntry> kvTable(
      final String name,
      final ResponsiveConfig config,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    Map<String, String> storeIdMapping = config.getMap(
        ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG);
    final String storeIdHex = storeIdMapping.get(name);
    if (storeIdHex == null) {
      throw new ConfigException("Failed to find store ID mapping for table " + name);
    }

    final UUID storeId = UUID.fromString(storeIdHex);
    final PssPartitioner pssPartitioner = new PssDirectPartitioner();
    final var rs3Client = connector.connect();
    return new RS3KVTable(
        name,
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
