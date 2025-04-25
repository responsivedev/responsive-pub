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

package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRs3Util.basicPutProto;
import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpcRs3Util.createStoreOptionsProto;
import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpsRs3TestUtil.newEndOfStreamResult;
import static dev.responsive.kafka.internal.utils.Utils.lssIdProto;
import static dev.responsive.kafka.internal.utils.Utils.uuidToProto;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.ClockType;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RS3TimeoutException;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import dev.responsive.rs3.Rs3.CreateStoreResult;
import dev.responsive.rs3.Rs3.ListStoresResult;
import dev.responsive.rs3.Rs3.StoreInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcRS3ClientTest {
  private static final String STORE_NAME = "my_store";
  private static final UUID STORE_ID = new UUID(100, 200);
  private static final LssId LSS_ID = new LssId(10);
  private static final int PSS_ID = 1;
  private static final int PSS_ID_2 = 2;

  @Mock
  private RS3Grpc.RS3BlockingStub stub;
  @Mock
  private RS3Grpc.RS3Stub asyncStub;
  @Mock
  private PssStubsProvider stubs;
  @Mock
  private StreamObserver<Rs3.WriteWALSegmentRequest> writeWALSegmentRequestObserver;
  @Captor
  private ArgumentCaptor<StreamObserver<Rs3.WriteWALSegmentResult>>
      writeWALSegmentResultObserverCaptor;
  private MockTime time = new MockTime();
  private long retryTimeoutMs = 30000;

  private GrpcRS3Client client;

  @BeforeEach
  public void setup() {
    lenient().when(stubs.stubs(any(), anyInt())).thenReturn(new PssStubsProvider.Stubs(
        stub,
        asyncStub
    ));
    lenient().when(stubs.globalStubs()).thenReturn(new PssStubsProvider.Stubs(
        stub,
        asyncStub
    ));
    client = new GrpcRS3Client(stubs, time, retryTimeoutMs);
  }

  @Test
  public void shouldReturnErrorOnGetOffsetsWithMissingFlushedOffset() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setWrittenOffset(GrpcRs3Util.walOffsetProto(123L))
            .build()
    );

    // when:
    assertThrows(RuntimeException.class, () -> client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID));
  }

  @Test
  public void shouldReturnErrorOnGetOffsetsWithMissingWrittenOffset() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setFlushedOffset(GrpcRs3Util.walOffsetProto(123L))
            .build()
    );

    // when:
    assertThrows(RuntimeException.class, () -> client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID));
  }

  @Test
  public void shouldReturnEmptyWrittenOffsetOnGetOffsetsWithWrittenOffsetNone() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setWrittenOffset(GrpcRs3Util.UNWRITTEN_WAL_OFFSET)
            .setFlushedOffset(GrpcRs3Util.walOffsetProto(123L))
            .build()
    );

    // when:
    final var offsets = client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID);

    // then:
    assertThat(offsets.writtenOffset(), is(Optional.empty()));
    assertThat(offsets.flushedOffset(), is(Optional.of(123L)));
  }

  @Test
  public void shouldReturnEmptyFlushedOffsetOnGetOffsetsWithFlushedOffsetNone() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setWrittenOffset(GrpcRs3Util.walOffsetProto(123L))
            .setFlushedOffset(GrpcRs3Util.UNWRITTEN_WAL_OFFSET)
            .build()
    );

    // when:
    final var offsets = client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID);

    // then:
    assertThat(offsets.writtenOffset(), is(Optional.of(123L)));
    assertThat(offsets.flushedOffset(), is(Optional.empty()));
  }

  @Test
  public void shouldGetOffsets() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setWrittenOffset(GrpcRs3Util.walOffsetProto(13))
            .setFlushedOffset(GrpcRs3Util.walOffsetProto(3))
            .build()
    );

    // when:
    final var offsets = client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID);

    // then:
    assertThat(offsets.writtenOffset(), is(Optional.of(13L)));
    assertThat(offsets.flushedOffset(), is(Optional.of(3L)));
  }

  @Test
  public void shouldRetryGetOffsets() {
    // given:
    when(stub.getOffsets(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(
            Rs3.GetOffsetsResult.newBuilder()
                .setWrittenOffset(GrpcRs3Util.walOffsetProto(13))
                .setFlushedOffset(GrpcRs3Util.walOffsetProto(3))
                .build());

    // when:
    final var offsets = client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID);

    // then:
    assertThat(offsets.writtenOffset(), is(Optional.of(13L)));
    assertThat(offsets.flushedOffset(), is(Optional.of(3L)));
  }

  @Test
  public void shouldPropagateUnexpectedExceptionFromGetOffsets() {
    // given:
    when(stub.getOffsets(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN));

    // when:
    final RS3Exception exception =
        assertThrows(RS3Exception.class, () -> client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID));

    // then:
    assertThat(exception.getCause(), is(instanceOf(StatusRuntimeException.class)));
    assertThat(((StatusRuntimeException) exception.getCause()).getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldTimeoutGetOffsets() {
    // given:
    var startTimeMs = time.milliseconds();
    when(stub.getOffsets(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    // when:
    assertThrows(RS3TimeoutException.class, () -> client.getCurrentOffsets(
        STORE_ID,
        LSS_ID,
        PSS_ID
    ));

    // then:
    var endTimeMs = time.milliseconds();
    assertThat(endTimeMs - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  public void shouldWriteWalEntriesWithExpectedFields() {
    // given:
    when(asyncStub.writeWALSegmentStream(any())).thenReturn(writeWALSegmentRequestObserver);
    final var senderReceiver = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20
    );
    final var sender = senderReceiver.sender();
    final var put1 = new Put("foo".getBytes(), "bar".getBytes());
    final var put2 = new Put("bar".getBytes(), "baz".getBytes());

    // when:
    sender.sendNext(put1);
    sender.sendNext(put2);
    sender.finish();

    // then:
    verify(writeWALSegmentRequestObserver).onNext(Rs3.WriteWALSegmentRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidToProto(STORE_ID))
        .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15))
        .setEndOffset(20)
        .setPut(basicPutProto(put1))
        .build()
    );
    verify(writeWALSegmentRequestObserver).onNext(Rs3.WriteWALSegmentRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidToProto(STORE_ID))
        .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15))
        .setEndOffset(20)
        .setPut(basicPutProto(put2))
        .build()
    );
    verify(writeWALSegmentRequestObserver).onCompleted();
  }

  @Test
  public void shouldWriteWalEntriesWithExpectedWrittenOffsetNone() {
    // given:
    when(asyncStub.writeWALSegmentStream(any())).thenReturn(writeWALSegmentRequestObserver);
    final var senderReceiver = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        20
    );
    final var sender = senderReceiver.sender();
    final var put1 = new Put("foo".getBytes(), "bar".getBytes());

    // when:
    sender.sendNext(put1);

    // then:
    verify(writeWALSegmentRequestObserver).onNext(Rs3.WriteWALSegmentRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidToProto(STORE_ID))
        .setExpectedWrittenOffset(GrpcRs3Util.UNWRITTEN_WAL_OFFSET)
        .setEndOffset(20)
        .setPut(basicPutProto(put1))
        .build()
    );
  }

  @Test
  public void shouldHandleWriteWalSegmentResponse()
      throws InterruptedException, ExecutionException {
    // given:
    when(asyncStub.writeWALSegmentStream(any())).thenReturn(writeWALSegmentRequestObserver);
    final var senderReceiver = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        20
    );
    final var receiveFuture = senderReceiver.completion().toCompletableFuture();
    final var observer = verifyWalSegmentResultObserver();
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(GrpcRs3Util.walOffsetProto(123))
        .build());
    observer.onCompleted();

    // when:
    final var result = receiveFuture.get();

    // then:
    assertThat(result.get(), is(123L));
  }

  @Test
  public void shouldThrowOnWriteWalSegmentError() {
    // given:
    when(asyncStub.writeWALSegmentStream(any())).thenReturn(writeWALSegmentRequestObserver);
    final var senderReceiver = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        20
    );
    final var receiveFuture = senderReceiver.completion().toCompletableFuture();
    final var observer = verifyWalSegmentResultObserver();
    observer.onError(new TestException());

    // when:
    final var thrown = assertThrows(ExecutionException.class, receiveFuture::get);

    // then:
    assertThat(thrown.getCause(), instanceOf(TestException.class));
  }

  @Test
  public void shouldThrowOnMultipleWriteWalSegmentResponseMessages() {
    // given:
    when(asyncStub.writeWALSegmentStream(any())).thenReturn(writeWALSegmentRequestObserver);
    final var senderReceiver = client.writeWalSegmentAsync(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.empty(),
        20
    );
    final var receiveFuture = senderReceiver.completion().toCompletableFuture();
    final var observer = verifyWalSegmentResultObserver();
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(GrpcRs3Util.walOffsetProto(123))
        .build());
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(GrpcRs3Util.walOffsetProto(456))
        .build());
    observer.onCompleted();

    // when:
    final var thrown = assertThrows(ExecutionException.class, receiveFuture::get);

    // then:
    assertThat(thrown.getCause(), instanceOf(IllegalStateException.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWriteWalSegmentSync() {
    // given:
    var flushedOffset = 123L;
    List<WalEntry> entries = Arrays.asList(
        new Put("foo".getBytes(), "bar".getBytes()),
        new Put("bar".getBytes(), "baz".getBytes())
    );
    when(asyncStub.writeWALSegmentStream(any())).thenAnswer(invocation -> {
      StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
          invocation.getArgument(0);
      responseObserver.onNext(Rs3.WriteWALSegmentResult.newBuilder()
                          .setFlushedOffset(GrpcRs3Util.walOffsetProto(flushedOffset))
                          .build());
      responseObserver.onCompleted();
      return writeWALSegmentRequestObserver;
    });

    // when:
    final var result = client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20,
        entries
    );

    // then:
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(0)))
            .build()
    );
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(1)))
            .build()
    );

    assertThat(result, is(Optional.of(flushedOffset)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryWriteWalSegmentSync() {
    // given:
    var flushedOffset = 123L;
    List<WalEntry> entries = Arrays.asList(
        new Put("foo".getBytes(), "bar".getBytes()),
        new Put("bar".getBytes(), "baz".getBytes())
    );
    when(asyncStub.writeWALSegmentStream(any()))
        .thenAnswer(invocation -> {
          StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
              invocation.getArgument(0);
          responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
          return writeWALSegmentRequestObserver;
        })
        .thenAnswer(invocation -> {
          StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
              invocation.getArgument(0);
          responseObserver.onNext(Rs3.WriteWALSegmentResult.newBuilder()
                              .setFlushedOffset(GrpcRs3Util.walOffsetProto(flushedOffset))
                              .build());
          responseObserver.onCompleted();
          reset(writeWALSegmentRequestObserver);
          return writeWALSegmentRequestObserver;
        });


    // when:
    final var result = client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20,
        entries
    );

    // then:
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(0)))
            .build()
    );
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(1)))
            .build()
    );
    assertThat(result, is(Optional.of(flushedOffset)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryWriteWalSegmentSyncWithErrorInOnNext() {
    // given:
    var flushedOffset = 123L;
    List<WalEntry> entries = Collections.singletonList(
        new Put("foo".getBytes(), "bar".getBytes())
    );

    doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .when(writeWALSegmentRequestObserver)
        .onNext(any());

    when(asyncStub.writeWALSegmentStream(any()))
        .thenAnswer(invocation -> writeWALSegmentRequestObserver)
        .thenAnswer(invocation -> {
          StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
              invocation.getArgument(0);
          responseObserver.onNext(Rs3.WriteWALSegmentResult.newBuilder()
                                      .setFlushedOffset(GrpcRs3Util.walOffsetProto(flushedOffset))
                                      .build());
          responseObserver.onCompleted();
          reset(writeWALSegmentRequestObserver);
          return writeWALSegmentRequestObserver;
        });


    // when:
    final var result = client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20,
        entries
    );

    // then:
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(0)))
            .build()
    );
    assertThat(result, is(Optional.of(flushedOffset)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryWriteWalSegmentSyncWithErrorInFinish() {
    // given:
    var flushedOffset = 123L;
    List<WalEntry> entries = Collections.singletonList(
        new Put("foo".getBytes(), "bar".getBytes())
    );

    doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .when(writeWALSegmentRequestObserver)
        .onCompleted();

    when(asyncStub.writeWALSegmentStream(any()))
        .thenAnswer(invocation -> writeWALSegmentRequestObserver)
        .thenAnswer(invocation -> {
          StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
              invocation.getArgument(0);
          responseObserver.onNext(Rs3.WriteWALSegmentResult.newBuilder()
                                      .setFlushedOffset(GrpcRs3Util.walOffsetProto(flushedOffset))
                                      .build());
          responseObserver.onCompleted();
          reset(writeWALSegmentRequestObserver);
          return writeWALSegmentRequestObserver;
        });

    // when:
    final var result = client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20,
        entries
    );

    // then:
    verify(writeWALSegmentRequestObserver).onNext(
        Rs3.WriteWALSegmentRequest.newBuilder()
            .setLssId(lssIdProto(LSS_ID))
            .setPssId(PSS_ID)
            .setStoreId(uuidToProto(STORE_ID))
            .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(15L))
            .setEndOffset(20)
            .setPut(basicPutProto((Put) entries.get(0)))
            .build()
    );
    assertThat(result, is(Optional.of(flushedOffset)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldTimeoutWriteWalSegmentSync() {
    // given:
    List<WalEntry> entries = Arrays.asList(
        new Put("foo".getBytes(), "bar".getBytes()),
        new Put("bar".getBytes(), "baz".getBytes())
    );
    when(asyncStub.writeWALSegmentStream(any()))
        .thenAnswer(invocation -> {
          StreamObserver<dev.responsive.rs3.Rs3.WriteWALSegmentResult> responseObserver =
              invocation.getArgument(0);
          responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
          return writeWALSegmentRequestObserver;
        });


    // when:
    var startTimeMs = time.milliseconds();
    assertThrows(RS3TimeoutException.class, () -> client.writeWalSegment(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(15L),
        20,
        entries
    ));

    // then:
    assertThat(time.milliseconds() - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  public void shouldGetWithExpectedWrittenOffset() {
    // given:
    when(stub.get(any())).thenReturn(
        Rs3.GetResult.newBuilder()
            .setResult(Rs3.KeyValue.newBuilder().setBasicKv(
                GrpcRs3Util.basicKeyValueProto(
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8))
                ))
            .build());

    // when:
    final var result = client.get(STORE_ID, LSS_ID, PSS_ID, Optional.of(123L), "foo".getBytes());

    // then:
    assertThat(result.get(), is("bar".getBytes()));
    verify(stub).get(Rs3.GetRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidToProto(STORE_ID))
        .setExpectedWrittenOffset(GrpcRs3Util.walOffsetProto(123L))
        .setKey(Rs3.Key.newBuilder().setBasicKey(
            GrpcRs3Util.basicKeyProto("foo".getBytes(StandardCharsets.UTF_8))
        ))
        .build()
    );
  }

  @Test
  public void shouldGet() {
    // given:
    when(stub.get(any())).thenReturn(
        Rs3.GetResult.newBuilder()
            .setResult(Rs3.KeyValue.newBuilder().setBasicKv(
                GrpcRs3Util.basicKeyValueProto(
                    "foo".getBytes(StandardCharsets.UTF_8),
                    "bar".getBytes(StandardCharsets.UTF_8)
                ))
            )
            .build()
    );

    // when:
    final var result = client.get(STORE_ID, LSS_ID, PSS_ID, Optional.empty(), "foo".getBytes());

    // then:
    assertThat(result.get(), is("bar".getBytes()));
    verify(stub).get(Rs3.GetRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidToProto(STORE_ID))
        .setKey(Rs3.Key.newBuilder().setBasicKey(
            GrpcRs3Util.basicKeyProto("foo".getBytes(StandardCharsets.UTF_8))
        ))
        .setExpectedWrittenOffset(GrpcRs3Util.UNWRITTEN_WAL_OFFSET)
        .build()
    );
  }

  @Test
  public void shouldHandleNegativeGet() {
    // given:
    when(stub.get(any())).thenReturn(
        Rs3.GetResult.newBuilder().build()
    );

    // when:
    final var result = client.get(STORE_ID, LSS_ID, PSS_ID, Optional.of(123L), "foo".getBytes());

    // then:
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldRetryGet() {
    // given:
    when(stub.get(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(Rs3.GetResult.newBuilder().build());

    // when:
    final var result = client.get(STORE_ID, LSS_ID, PSS_ID, Optional.of(123L), "foo".getBytes());

    // then:
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldPropagateUnexpectedExceptionsFromGet() {
    // given:
    when(stub.get(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN));

    // when:
    final RS3Exception exception = assertThrows(
        RS3Exception.class,
        () -> client.get(STORE_ID, LSS_ID, PSS_ID, Optional.of(123L), "foo".getBytes())
    );

    // then:
    assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
    assertThat(((StatusRuntimeException) exception.getCause()).getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldTimeoutGet() {
    // given:
    var startTimeMs = time.milliseconds();
    when(stub.get(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    // when:
    assertThrows(RS3TimeoutException.class, () -> client.get(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        "foo".getBytes()
    ));

    // then:
    var endTimeMs = time.milliseconds();
    assertThat(endTimeMs - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryRangeRequest() {
    final var attemptCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      if (attemptCount.incrementAndGet() < 3) {
        throw new StatusRuntimeException(Status.UNAVAILABLE);
      } else {
        observer.onNext(newEndOfStreamResult());
      }
      return null;
    }).when(asyncStub).range(any(), any());

    try (final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        Range.unbounded()
    )) {
      assertThat(iter.hasNext(), is(false));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryAfterObserverOnError() {
    final var attemptCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      if (attemptCount.getAndIncrement() == 0) {
        observer.onError(new StatusRuntimeException(Status.UNAVAILABLE));
      } else {
        observer.onNext(newEndOfStreamResult());
      }
      return null;
    }).when(asyncStub).range(any(), any());

    final var startTimeMs = time.milliseconds();
    try (final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        Range.unbounded()
    )) {
      assertThat(iter.hasNext(), is(false));
    }
    // Expect some backoff after the retry.
    assertThat(time.milliseconds(), greaterThan(startTimeMs));
  }

  @Test
  public void shouldTimeoutRangeRequest() {
    doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .when(asyncStub)
        .range(any(), any());

    final var startTimeMs = time.milliseconds();
    assertThrows(RS3TimeoutException.class, () -> client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        Range.unbounded()
    ));
    var endTimeMs = time.milliseconds();
    assertThat(endTimeMs - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  public void shouldPropagateUnexpectedErrorInRangeRequest() {
    doThrow(new StatusRuntimeException(Status.UNKNOWN))
        .when(asyncStub)
        .range(any(), any());

    final var rs3Exception = assertThrows(RS3Exception.class, () -> client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        Range.unbounded()
    ));

    assertThat(rs3Exception.getCause(), is(instanceOf(StatusRuntimeException.class)));
    assertThat(
        ((StatusRuntimeException) rs3Exception.getCause()).getStatus().getCode(),
        is(Status.Code.UNKNOWN)
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldTimeoutAfterObserverOnError() {
    doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onError(new StatusRuntimeException(Status.UNAVAILABLE));
      return null;
    }).when(asyncStub).range(any(), any());

    try (
        final var iter = client.range(
            STORE_ID,
            LSS_ID,
            PSS_ID,
            Optional.of(123L),
            Range.unbounded()
        )
    ) {
      assertThrows(RS3TimeoutException.class, iter::hasNext);
    }
  }

  @Test
  public void shouldListStores() {
    // given:
    when(stub.listStores(any())).thenReturn(
        ListStoresResult.newBuilder()
            .addStores(StoreInfo.newBuilder()
                           .setStoreName(STORE_NAME)
                           .setStoreId(uuidToProto(STORE_ID))
                           .addAllPssIds(List.of(PSS_ID, PSS_ID_2))
                           .build()
            ).build()
    );

    // when:
    final var result = client.listStores();

    // then:
    final var expected = new dev.responsive.kafka.internal.db.rs3.client.Store(
        STORE_NAME, STORE_ID, List.of(PSS_ID, PSS_ID_2)
    );

    assertThat(result.size(), is(1));
    assertThat(result.get(0), equalTo(expected));
    verify(stub).listStores(Rs3.ListStoresRequest.newBuilder().build());
  }

  @Test
  public void shouldHandleEmptyStoresList() {
    // given:
    when(stub.listStores(any()))
        .thenReturn(ListStoresResult.newBuilder().build());

    // when:
    final var result = client.listStores();

    // then:
    assertThat(result.size(), is(0));
    verify(stub).listStores(Rs3.ListStoresRequest.newBuilder().build());
  }

  @Test
  public void shouldRetryListStores() {
    // given:
    when(stub.listStores(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(ListStoresResult.newBuilder().build());

    // when:
    final var result = client.listStores();

    // then:
    assertThat(result.size(), is(0));
    verify(stub, times(2)).listStores(Rs3.ListStoresRequest.newBuilder().build());
  }

  @Test
  public void shouldPropagateUnexpectedExceptionsFromListStores() {
    // given:
    when(stub.listStores(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN));

    // when:
    final RS3Exception exception = assertThrows(
        RS3Exception.class,
        () -> client.listStores()
    );

    // then:
    assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
    assertThat(((StatusRuntimeException) exception.getCause()).getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldTimeoutListStores() {
    // given:
    var startTimeMs = time.milliseconds();
    when(stub.listStores(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    // when:
    assertThrows(RS3TimeoutException.class, () -> client.listStores());

    // then:
    var endTimeMs = time.milliseconds();
    assertThat(endTimeMs - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  public void shouldCreateStore() {
    // given:
    final int logicalShards = 5;
    final var pss_ids = IntStream.range(0, logicalShards - 1).boxed().collect(Collectors.toList());
    when(stub.createStore(any()))
        .thenReturn(CreateStoreResult
                        .newBuilder()
                        .setStoreId(uuidToProto(STORE_ID))
                        .addAllPssIds(pss_ids)
                        .build()
        );

    final CreateStoreOptions options = new CreateStoreOptions(
        logicalShards,
        CreateStoreTypes.StoreType.BASIC,
        Optional.of(ClockType.STREAM_TIME),
        Optional.of(10_000L),
        Optional.empty()
    );

    // when:
    final var result = client.createStore(STORE_NAME, options);

    // then:
    assertThat(result, equalTo(new CreateStoreTypes.CreateStoreResult(STORE_ID, pss_ids)));
    verify(stub).createStore(
        Rs3.CreateStoreRequest.newBuilder()
            .setStoreName(STORE_NAME)
            .setOptions(createStoreOptionsProto(options))
            .build()
    );
  }

  @Test
  public void shouldRetryCreateStore() {
    // given:
    final int logicalShards = 5;
    final var pss_ids = IntStream.range(0, logicalShards - 1).boxed().collect(Collectors.toList());
    when(stub.createStore(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(CreateStoreResult
                        .newBuilder()
                        .setStoreId(uuidToProto(STORE_ID))
                        .addAllPssIds(pss_ids)
                        .build()
        );

    final var slateDbOptions = new CreateStoreTypes.SlateDbStorageOptions(
        Optional.of(20)
    );

    final var options = new CreateStoreOptions(
        logicalShards,
        CreateStoreTypes.StoreType.BASIC,
        Optional.empty(),
        Optional.of(10_000L),
        Optional.of(slateDbOptions)
    );

    // when:
    final var result = client.createStore(STORE_NAME, options);

    // then:
    assertThat(result, equalTo(new CreateStoreTypes.CreateStoreResult(STORE_ID, pss_ids)));
    verify(stub, times(2)).createStore(
        Rs3.CreateStoreRequest.newBuilder()
            .setStoreName(STORE_NAME)
            .setOptions(createStoreOptionsProto(options))
            .build()
    );
  }

  @Test
  public void shouldPropagateUnexpectedExceptionsFromCreateStore() {
    // given:
    when(stub.createStore(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN));

    final int logicalShards = 5;
    final var slateDbOptions = new CreateStoreTypes.SlateDbStorageOptions(
        Optional.of(20)
    );

    final CreateStoreOptions options = new CreateStoreOptions(
        logicalShards,
        CreateStoreTypes.StoreType.BASIC,
        Optional.empty(),
        Optional.of(10_000L),
        Optional.of(slateDbOptions)
    );

    // when:
    final RS3Exception exception = assertThrows(
        RS3Exception.class,
        () -> client.createStore(STORE_NAME, options)
    );

    // then:
    assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
    assertThat(((StatusRuntimeException) exception.getCause()).getStatus(), is(Status.UNKNOWN));
  }

  @Test
  public void shouldTimeoutCreateStore() {
    // given:
    var startTimeMs = time.milliseconds();
    when(stub.createStore(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    final int logicalShards = 5;

    final var slateDbOptions = new CreateStoreTypes.SlateDbStorageOptions(
        Optional.of(20)
    );
    final CreateStoreOptions options = new CreateStoreOptions(
        logicalShards,
        CreateStoreTypes.StoreType.BASIC,
        Optional.empty(),
        Optional.of(10_000L),
        Optional.of(slateDbOptions)
    );

    // when:
    assertThrows(
        RS3TimeoutException.class,
        () -> client.createStore(STORE_NAME, options)
    );

    // then:
    var endTimeMs = time.milliseconds();
    assertThat(endTimeMs - startTimeMs, is(retryTimeoutMs));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPropagateUnexpectedExceptionFromObserverOnError() {
    doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onError(new StatusRuntimeException(Status.UNKNOWN));
      return null;
    }).when(asyncStub).range(any(), any());

    try (final var iter = client.range(
        STORE_ID,
        LSS_ID,
        PSS_ID,
        Optional.of(123L),
        Range.unbounded()
    )) {
      final var rs3Exception = assertThrows(RS3Exception.class, iter::hasNext);
      assertThat(rs3Exception.getCause(), is(instanceOf(StatusRuntimeException.class)));
      assertThat(
          ((StatusRuntimeException) rs3Exception.getCause()).getStatus().getCode(),
          is(Status.Code.UNKNOWN)
      );
    }
  }

  private StreamObserver<Rs3.WriteWALSegmentResult> verifyWalSegmentResultObserver() {
    verify(asyncStub).writeWALSegmentStream(writeWALSegmentResultObserverCaptor.capture());
    return writeWALSegmentResultObserverCaptor.getValue();
  }

  public static class TestException extends RuntimeException {
    private static final long serialVersionUID = 0L;
  }
}