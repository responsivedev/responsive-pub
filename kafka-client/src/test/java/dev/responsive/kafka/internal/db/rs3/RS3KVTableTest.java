package dev.responsive.kafka.internal.db.rs3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3RetryUtil;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RS3KVTableTest {
  private final MockTime time = new MockTime();

  @Mock
  private RS3Client client;

  @Mock
  private PssPartitioner partitioner;

  @Mock
  private ResponsiveMetrics metrics;


  private ResponsiveMetrics.MetricScopeBuilder metricsScopeBuilder =
      new ResponsiveMetrics.MetricScopeBuilder(new LinkedHashMap<>());


  @Test
  public void shouldInitializeWrittenOffset() {
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var writtenOffset = 100L;

    Mockito.when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    Mockito.when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(new CurrentOffsets(Optional.of(writtenOffset), Optional.empty()));
    Mockito.when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    var table = new RS3KVTable(
        "store",
        storeId,
        client,
        new RS3RetryUtil(3000, time),
        partitioner,
        metrics,
        metricsScopeBuilder
    );

    var flushManager = (RS3KVFlushManager) table.init(kafkaPartition);
    assertThat(flushManager.writtenOffset(pssId), is(Optional.of(writtenOffset)));
  }

}