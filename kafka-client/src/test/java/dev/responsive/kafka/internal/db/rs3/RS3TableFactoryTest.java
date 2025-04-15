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

import static dev.responsive.kafka.internal.stores.TtlResolver.NO_TTL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client.Connector;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RS3TableFactoryTest {
  private final ResponsiveMetrics metrics = new ResponsiveMetrics(new Metrics());
  private ResponsiveMetrics.MetricScopeBuilder scopeBuilder;

  @Mock
  private GrpcRS3Client client;

  @BeforeEach
  public void setup() {
    metrics.initializeTags(
        "applicationId",
        "streamsClientId",
        new ClientVersionMetadata(
            "responsiveClientVersion",
            "responsiveClientCommitId",
            "streamsClientVersion",
            "streamsClientCommitId"
        ),
        Map.of()
    );
    scopeBuilder = metrics.storeLevelMetricScopeBuilder(
        "thread",
        new TopicPartition("t", 0),
        "store"
    );
  }

  @Test
  public void testTableMapping() {
    final var table = "test-table";
    final var uuid = "b1a45157-e2f0-4698-be0e-5bf3a9b8e9d1";
    final int partitions = 5;

    final var config = mock(ResponsiveConfig.class);
    when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.singletonMap(table, uuid));

    when(client.createStore(any(UUID.class), anyInt(), any(CreateStoreOptions.class)))
        .thenReturn(List.of(1, 2, 3, 4, 5));

    final RS3TableFactory factory = newTestFactory();
    final RS3KVTable rs3Table = (RS3KVTable) factory.kvTable(
        table,
        config,
        NO_TTL,
        metrics,
        scopeBuilder,
        () -> partitions
    );
    assertEquals(uuid, rs3Table.storedId().toString());

    verify(client).createStore(
        UUID.fromString(uuid),
        partitions,
        new CreateStoreOptions(Optional.empty(), Optional.empty(), Optional.empty())
    );
  }

  @Test
  public void testMissingTableMapping() {
    final String table = "test-table";
    final ResponsiveConfig config = mock(ResponsiveConfig.class);
    when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.emptyMap());

    final RS3TableFactory factory = newTestFactory();
    assertThrows(
        ConfigException.class,
        () -> factory.kvTable(table, config, NO_TTL, metrics, scopeBuilder, () -> 5)
    );
  }

  private RS3TableFactory newTestFactory() {
    final var connector = mock(Connector.class);
    lenient().when(connector.connect()).thenReturn(client);
    return new RS3TableFactory(connector);
  }

}
