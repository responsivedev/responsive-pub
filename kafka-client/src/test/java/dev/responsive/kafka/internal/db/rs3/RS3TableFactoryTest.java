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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.config.ResponsiveConfig.DefaultRS3ConfigSetter;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreResult;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.SlateDbStorageOptions;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client.Connector;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
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
  public void testBasicKeyValueTableMapping() {
    final UUID storeId = new UUID(100, 200);
    final String tableName = "test-table";
    final int partitions = 5;

    when(client.createStore(anyString(), any(CreateStoreOptions.class)))
        .thenReturn(new CreateStoreResult(storeId, List.of(1, 2, 3, 4, 5)));

    final RS3TableFactory factory = newTestFactory();
    final RS3KVTable rs3Table = (RS3KVTable) factory.kvTable(
        tableName,
        NO_TTL,
        metrics,
        scopeBuilder,
        () -> partitions,
        KVSchema.KEY_VALUE
    );
    assertEquals(tableName, rs3Table.name());
    assertEquals(storeId, rs3Table.storeId());

    final var expectedOptions = new CreateStoreOptions(
        partitions,
        CreateStoreTypes.StoreType.BASIC,
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
    verify(client).createStore(tableName, expectedOptions);
  }

  @Test
  public void testFactTableMapping() {
    final UUID storeId = new UUID(100, 200);
    final String tableName = "test-table";
    final int partitions = 5;

    when(client.createStore(anyString(), any(CreateStoreOptions.class)))
        .thenReturn(new CreateStoreResult(storeId, List.of(1, 2, 3, 4, 5)));

    final RS3TableFactory factory = newTestFactory();
    final RS3KVTable rs3Table = (RS3KVTable) factory.kvTable(
        tableName,
        NO_TTL,
        metrics,
        scopeBuilder,
        () -> partitions,
        KVSchema.FACT
    );
    assertEquals(tableName, rs3Table.name());
    assertEquals(storeId, rs3Table.storeId());

    final var expectedStorageOptions = Optional.of(new SlateDbStorageOptions(Optional.of(20)));
    final var expectedOptions = new CreateStoreOptions(
        partitions,
        CreateStoreTypes.StoreType.BASIC,
        Optional.empty(),
        Optional.empty(),
        expectedStorageOptions
    );
    verify(client).createStore(tableName, expectedOptions);
  }

  @Test
  public void testWindowTableMapping() {
    final UUID storeId = new UUID(100, 200);
    final String tableName = "test-table";
    final int partitions = 5;

    when(client.createStore(anyString(), any(CreateStoreOptions.class)))
        .thenReturn(new CreateStoreResult(storeId, List.of(1, 2, 3, 4, 5)));

    final var factory = newTestFactory();
    final var defaultTtl = Duration.ofMinutes(10);
    final RS3WindowTable rs3Table = (RS3WindowTable) factory.windowTable(
        tableName,
        defaultTtl,
        metrics,
        scopeBuilder,
        () -> partitions,
        WindowSchema.WINDOW
    );
    assertEquals(tableName, rs3Table.name());
    assertEquals(storeId, rs3Table.storeId());

    final var expectedOptions = new CreateStoreOptions(
        partitions,
        CreateStoreTypes.StoreType.WINDOW,
        Optional.of(CreateStoreTypes.ClockType.WALL_CLOCK),
        Optional.of(defaultTtl.toMillis()),
        Optional.empty()
    );
    verify(client).createStore(tableName, expectedOptions);
  }

  private RS3TableFactory newTestFactory() {
    final var connector = mock(Connector.class);
    lenient().when(connector.connect()).thenReturn(client);
    return new RS3TableFactory(connector, new DefaultRS3ConfigSetter());
  }

}
