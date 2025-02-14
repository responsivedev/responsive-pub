package dev.responsive.kafka.internal.db.rs3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRS3Client;
import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RS3TableFactoryTest {
  private final ResponsiveMetrics metrics = new ResponsiveMetrics(new Metrics());
  private ResponsiveMetrics.MetricScopeBuilder scopeBuilder;


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
    var table = "test-table";
    var uuid = "b1a45157-e2f0-4698-be0e-5bf3a9b8e9d1";

    var config = Mockito.mock(ResponsiveConfig.class);
    Mockito.when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.singletonMap(table, uuid));

    final RS3TableFactory factory = newTestFactory();
    final RS3KVTable rs3Table = (RS3KVTable) factory.kvTable(
        table,
        config,
        metrics,
        scopeBuilder
    );
    assertEquals(uuid, rs3Table.storedId().toString());
  }

  @Test
  public void testMissingTableMapping() {
    String table = "test-table";
    ResponsiveConfig config = Mockito.mock(ResponsiveConfig.class);
    Mockito.when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.emptyMap());

    final RS3TableFactory factory = newTestFactory();
    assertThrows(
        ConfigException.class,
        () -> factory.kvTable(table, config, metrics, scopeBuilder)
    );
  }

  private RS3TableFactory newTestFactory() {
    final var connector = new GrpcRS3Client.Connector(new MockTime(), "localhost", 50051);
    return new RS3TableFactory(connector);
  }

}