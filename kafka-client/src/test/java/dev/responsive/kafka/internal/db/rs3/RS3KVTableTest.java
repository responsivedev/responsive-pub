package dev.responsive.kafka.internal.db.rs3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3TimeoutException;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
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


  private final ResponsiveMetrics.MetricScopeBuilder metricsScopeBuilder =
      new ResponsiveMetrics.MetricScopeBuilder(new LinkedHashMap<>());


  @Test
  public void shouldInitializeWrittenOffset() {
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var writtenOffset = 100L;

    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.of(writtenOffset), Optional.empty()));
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    var table = new RS3KVTable(
        "store",
        storeId,
        client,
        partitioner,
        metrics,
        metricsScopeBuilder
    );

    var flushManager = (RS3KVFlushManager) table.init(kafkaPartition);
    assertThat(flushManager.writtenOffset(pssId), is(Optional.of(writtenOffset)));
  }

  @Test
  public void shouldRetryAfterTransientErrorInWalSegmentStream() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    @SuppressWarnings("unchecked")
    StreamSender<WalEntry> streamSender = Mockito.mock(StreamSender.class);
    CompletableFuture<Optional<Long>> receiveCompletion = new CompletableFuture<>();

    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(
        storeId,
        lssId,
        pssId,
        Optional.empty(),
        consumedOffset))
        .thenReturn(new StreamSenderMessageReceiver<>(streamSender, receiveCompletion));
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    // When:
    receiveCompletion.completeExceptionally(new RS3TransientException(
        new StatusRuntimeException(Status.UNAVAILABLE)));
    Mockito.doThrow(new IllegalStateException())
        .when(streamSender)
        .sendNext(any());
    when(client.writeWalSegment(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.empty()),
        eq(consumedOffset),
        anyList()))
        .thenReturn(Optional.of(consumedOffset));

    // Then:
    var table = new RS3KVTable(
        "store",
        storeId,
        client,
        partitioner,
        metrics,
        metricsScopeBuilder
    );
    var flushManager = (RS3KVFlushManager) table.init(kafkaPartition);
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());

    // Then:
    var result = writer.flush().toCompletableFuture().join();
    assertThat(result.wasApplied(), is(true));
    assertThat(result.tablePartition(), is(pssId));
  }

  @Test
  public void shouldRetryAfterTransientErrorInWalSegmentStreamFinish() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    @SuppressWarnings("unchecked")
    StreamSender<WalEntry> streamSender = Mockito.mock(StreamSender.class);
    CompletableFuture<Optional<Long>> receiveCompletion = new CompletableFuture<>();

    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(
        storeId,
        lssId,
        pssId,
        Optional.empty(),
        consumedOffset))
        .thenReturn(new StreamSenderMessageReceiver<>(streamSender, receiveCompletion));
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    // When:
    receiveCompletion.completeExceptionally(new RS3TransientException(
        new StatusRuntimeException(Status.UNAVAILABLE)));
    Mockito.doThrow(new IllegalStateException())
        .when(streamSender)
        .finish();
    when(client.writeWalSegment(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.empty()),
        eq(consumedOffset),
        anyList()))
        .thenReturn(Optional.of(consumedOffset));

    // Then:
    var table = new RS3KVTable(
        "store",
        storeId,
        client,
        partitioner,
        metrics,
        metricsScopeBuilder
    );
    var flushManager = (RS3KVFlushManager) table.init(kafkaPartition);
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
    verify(streamSender).sendNext(any());
    var result = writer.flush().toCompletableFuture().join();
    assertThat(result.wasApplied(), is(true));
    assertThat(result.tablePartition(), is(pssId));
  }

  @Test
  public void shouldRaiseTimeoutFromRetry() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    @SuppressWarnings("unchecked")
    StreamSender<WalEntry> streamSender = Mockito.mock(StreamSender.class);
    CompletableFuture<Optional<Long>> receiveCompletion = new CompletableFuture<>();

    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(
        storeId,
        lssId,
        pssId,
        Optional.empty(),
        consumedOffset))
        .thenReturn(new StreamSenderMessageReceiver<>(streamSender, receiveCompletion));
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    // When:
    receiveCompletion.completeExceptionally(new RS3TransientException(
        new StatusRuntimeException(Status.UNAVAILABLE)));
    Mockito.doThrow(new IllegalStateException())
        .when(streamSender)
        .sendNext(any());
    when(client.writeWalSegment(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.empty()),
        eq(consumedOffset),
        anyList()))
        .thenThrow(new RS3TimeoutException("Timeout in retry"));

    // Then:
    var table = new RS3KVTable(
        "store",
        storeId,
        client,
        partitioner,
        metrics,
        metricsScopeBuilder
    );

    var flushManager = (RS3KVFlushManager) table.init(kafkaPartition);
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
    var exception = assertThrows(
        CompletionException.class,
        () -> writer.flush().toCompletableFuture().join()
    );
    assertThat(exception.getCause(), instanceOf(RS3TimeoutException.class));
  }

}