package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcRS3ClientTest {
  private static final UUID STORE_ID = new UUID(100, 200);
  private static final LssId LSS_ID = new LssId(10);
  private static final int PSS_ID = 1;

  @Mock
  private RS3Grpc.RS3BlockingStub stub;
  @Mock
  private RS3Grpc.RS3Stub asyncStub;
  @Mock
  private ManagedChannel channel;
  @Mock
  private StreamObserver<Rs3.WriteWALSegmentRequest> writeWALSegmentRequestObserver;
  @Captor
  private ArgumentCaptor<StreamObserver<Rs3.WriteWALSegmentResult>>
      writeWALSegmentResultObserverCaptor;

  private GrpcRS3Client client;

  @BeforeEach
  public void setup() {
    client = new GrpcRS3Client(channel, stub, asyncStub);
  }

  @Test
  public void shouldReturnErrorOnGetOffsetsWithMissingFlushedOffset() {
    // given:
    when(stub.getOffsets(any())).thenReturn(
        Rs3.GetOffsetsResult.newBuilder()
            .setWrittenOffset(123)
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
            .setFlushedOffset(123)
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
            .setWrittenOffset(GrpcRS3Client.WAL_OFFSET_NONE)
            .setFlushedOffset(123)
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
            .setWrittenOffset(123)
            .setFlushedOffset(GrpcRS3Client.WAL_OFFSET_NONE)
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
            .setWrittenOffset(13)
            .setFlushedOffset(3)
            .build()
    );

    // when:
    final var offsets = client.getCurrentOffsets(STORE_ID, LSS_ID, PSS_ID);

    // then:
    assertThat(offsets.writtenOffset(), is(Optional.of(13L)));
    assertThat(offsets.flushedOffset(), is(Optional.of(3L)));
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
        .setStoreId(uuidProto(STORE_ID))
        .setExpectedWrittenOffset(15L)
        .setEndOffset(20)
        .setPut(putProto(put1))
        .build()
    );
    verify(writeWALSegmentRequestObserver).onNext(Rs3.WriteWALSegmentRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidProto(STORE_ID))
        .setExpectedWrittenOffset(15L)
        .setEndOffset(20)
        .setPut(putProto(put2))
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
        .setStoreId(uuidProto(STORE_ID))
        .setExpectedWrittenOffset(GrpcRS3Client.WAL_OFFSET_NONE)
        .setEndOffset(20)
        .setPut(putProto(put1))
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
    final var receiveFuture = senderReceiver.receiver().toCompletableFuture();
    final var observer = verifyWalSegmentResultObserver();
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(123)
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
    final var receiveFuture = senderReceiver.receiver().toCompletableFuture();
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
    final var receiveFuture = senderReceiver.receiver().toCompletableFuture();
    final var observer = verifyWalSegmentResultObserver();
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(123)
        .build());
    observer.onNext(Rs3.WriteWALSegmentResult.newBuilder()
        .setFlushedOffset(456)
        .build());
    observer.onCompleted();

    // when:
    final var thrown = assertThrows(ExecutionException.class, receiveFuture::get);

    // then:
    assertThat(thrown.getCause(), instanceOf(IllegalStateException.class));
  }

  @Test
  public void shouldGetWithExpectedWrittenOffset() {
    // given:
    when(stub.get(any())).thenReturn(
        Rs3.GetResult.newBuilder()
            .setResult(Rs3.KeyValue.newBuilder()
                .setKey(ByteString.copyFromUtf8("foo"))
                .setValue(ByteString.copyFromUtf8("bar"))
            )
            .build()
    );

    // when:
    final var result = client.get(STORE_ID, LSS_ID, PSS_ID, Optional.of(123L), "foo".getBytes());

    // then:
    assertThat(result.get(), is("bar".getBytes()));
    verify(stub).get(Rs3.GetRequest.newBuilder()
        .setLssId(lssIdProto(LSS_ID))
        .setPssId(PSS_ID)
        .setStoreId(uuidProto(STORE_ID))
        .setExpectedWrittenOffset(123L)
        .setKey(ByteString.copyFromUtf8("foo"))
        .build()
    );
  }

  @Test
  public void shouldGet() {
    // given:
    when(stub.get(any())).thenReturn(
        Rs3.GetResult.newBuilder()
            .setResult(Rs3.KeyValue.newBuilder()
                .setKey(ByteString.copyFromUtf8("foo"))
                .setValue(ByteString.copyFromUtf8("bar"))
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
        .setStoreId(uuidProto(STORE_ID))
        .setKey(ByteString.copyFromUtf8("foo"))
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

  private StreamObserver<Rs3.WriteWALSegmentResult> verifyWalSegmentResultObserver() {
    verify(asyncStub).writeWALSegmentStream(writeWALSegmentResultObserverCaptor.capture());
    return writeWALSegmentResultObserverCaptor.getValue();
  }

  private Rs3.UUID uuidProto(final UUID uuid) {
    return Rs3.UUID.newBuilder()
        .setHigh(uuid.getMostSignificantBits())
        .setLow(uuid.getLeastSignificantBits())
        .build();
  }

  private Rs3.LSSId lssIdProto(final LssId lssId) {
    return Rs3.LSSId.newBuilder()
        .setId(lssId.id())
        .build();
  }

  private Rs3.WriteWALSegmentRequest.Put putProto(final Put put) {
    final var builder = Rs3.WriteWALSegmentRequest.Put.newBuilder()
        .setKey(ByteString.copyFrom(put.key()));
    if (put.value().isPresent()) {
      builder.setValue(ByteString.copyFrom(put.value().get()));
    }
    return builder.build();
  }

  public static class TestException extends RuntimeException {
    private static final long serialVersionUID = 0L;
  }
}