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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
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
import java.util.List;
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

  @Mock
  private StreamSender<WalEntry> streamSender;

  @Mock
  private StreamSenderMessageReceiver<WalEntry, Optional<Long>> sendRecv;

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
  public void shouldRetryAfterTransientErrorInFinish() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.sender()).thenReturn(streamSender);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(storeId, lssId, pssId, Optional.empty(), consumedOffset))
        .thenReturn(sendRecv);
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

    // When:
    Mockito.doAnswer(invocation -> {
      var exception = new RS3TransientException(new StatusRuntimeException(Status.UNAVAILABLE));
      sendRecvCompletion.completeExceptionally(exception);
      return null;
    })
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
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
    var result = writer.flush().toCompletableFuture().join();
    assertThat(result.wasApplied(), is(true));
    assertThat(result.tablePartition(), is(pssId));
  }

  @Test
  public void shouldRetryAfterTransientErrorInSendNextWalSegment() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.sender()).thenReturn(streamSender);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(storeId, lssId, pssId, Optional.empty(), consumedOffset))
        .thenReturn(sendRecv);
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

    // When:
    Mockito.doAnswer(invocation -> {
      var exception = new RS3TransientException(new StatusRuntimeException(Status.UNAVAILABLE));
      sendRecvCompletion.completeExceptionally(exception);
      return null;
    })
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
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
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

    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(storeId, lssId, pssId, Optional.empty(), consumedOffset))
        .thenReturn(sendRecv);
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));

    // When:
    sendRecvCompletion.completeExceptionally(new RS3TransientException(
        new StatusRuntimeException(Status.UNAVAILABLE)));
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
    var exception = assertThrows(CompletionException.class,
        () -> writer.flush().toCompletableFuture().join()
    );
    assertThat(exception.getCause(), instanceOf(RS3TimeoutException.class));
  }

  @Test
  public void shouldPropagateUnexpectedExceptionFromRetry() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
    when(partitioner.pssForLss(lssId))
        .thenReturn(Collections.singletonList(pssId));
    when(client.getCurrentOffsets(storeId, lssId, pssId))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    when(client.writeWalSegmentAsync(storeId, lssId, pssId, Optional.empty(), consumedOffset))
        .thenReturn(sendRecv);
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

    // When:
    sendRecvCompletion.completeExceptionally(new RS3TransientException(
        new StatusRuntimeException(Status.UNAVAILABLE)));
    when(client.writeWalSegment(
        eq(storeId),
        eq(lssId),
        eq(pssId),
        eq(Optional.empty()),
        eq(consumedOffset),
        anyList()))
        .thenThrow(new RS3Exception(new StatusRuntimeException(Status.UNKNOWN)));

    // Then:
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
    var exception = assertThrows(CompletionException.class,
        () -> writer.flush().toCompletableFuture().join()
    );
    assertThat(exception.getCause(), instanceOf(RS3Exception.class));
    RS3Exception rs3Exception = (RS3Exception) exception.getCause();
    assertThat(rs3Exception.getCause(), instanceOf(StatusRuntimeException.class));
    StatusRuntimeException statusRuntimeException =
        (StatusRuntimeException) rs3Exception.getCause();
    assertThat(statusRuntimeException.getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldPropagateUnexpectedExceptionFromStream() {
    // Given:
    var storeId = UUID.randomUUID();
    var kafkaPartition = 0;
    var lssId = new LssId(kafkaPartition);
    var pssId = kafkaPartition;
    var consumedOffset = 3L;

    final var sendRecvCompletion = new CompletableFuture<Optional<Long>>();
    when(sendRecv.completion()).thenReturn(sendRecvCompletion);
    when(sendRecv.sender()).thenReturn(streamSender);
    when(sendRecv.isActive()).thenAnswer(invocation -> !sendRecvCompletion.isDone());
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
        .thenReturn(sendRecv);
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

    // When:
    Mockito.doAnswer(invocation -> {
      var exception = new RS3Exception(new StatusRuntimeException(Status.UNKNOWN));
      sendRecvCompletion.completeExceptionally(exception);
      return null;
    })
        .when(streamSender)
        .sendNext(any());

    // Then:
    var writer = flushManager.createWriter(pssId, consumedOffset);
    var key = Bytes.wrap(new byte[] { 0 });
    var value = new byte[] { 0 };
    writer.insert(key, value, time.milliseconds());
    final var completion = writer.flush();

    var completionException = assertThrows(
        CompletionException.class,
        () -> completion.toCompletableFuture().join()
    );
    assertThat(completionException.getCause(), instanceOf(RS3Exception.class));
    var rs3Exception = (RS3Exception) completionException.getCause();
    assertThat(rs3Exception.getCause(), instanceOf(StatusRuntimeException.class));
    StatusRuntimeException statusRuntimeException =
        (StatusRuntimeException) rs3Exception.getCause();
    assertThat(statusRuntimeException.getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldCreateCheckpoint() {
    // given:
    final var storeId = UUID.randomUUID();
    final int partition = 1;
    final var pssCheckpoint = new PssCheckpoint(
        storeId,
        partition,
        new PssCheckpoint.SlateDbStorageCheckpoint(
            "/foo/bar",
            UUID.randomUUID()
        )
    );
    when(metrics.addSensor(any()))
        .thenReturn(Mockito.mock(Sensor.class));
    when(client.createCheckpoint(any(), any(), anyInt(), any())).thenReturn(pssCheckpoint);
    var lssId = new LssId(partition);
    when(partitioner.pssForLss(lssId)).thenReturn(List.of(partition));
    when(client.getCurrentOffsets(storeId, lssId, partition))
        .thenReturn(new CurrentOffsets(Optional.empty(), Optional.empty()));
    final var table = new RS3KVTable(
        "store",
        storeId,
        client,
        partitioner,
        metrics,
        metricsScopeBuilder
    );
    table.init(partition);

    // when:
    final byte[] serializedCheckpoint = table.checkpoint();

    // then:
    verify(client).createCheckpoint(
        storeId, new LssId(partition), partition, Optional.empty());
    final TableCheckpoint checkpoint = TableCheckpoint.deserialize(serializedCheckpoint);
    assertThat(
        checkpoint,
        is(
            new TableCheckpoint(
                List.of(
                    new TableCheckpoint.TablePssCheckpoint(
                        Optional.empty(),
                        pssCheckpoint
                    )
                )
            )
        )
    );
  }
}