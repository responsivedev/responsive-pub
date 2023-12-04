package dev.responsive.kafka.internal.db.otterpocket;

import static responsive.otterpocket.v1.service.Otterpockets.QueryRequest;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import responsive.otterpocket.v1.service.OtterPocketGrpcServiceGrpc;
import responsive.otterpocket.v1.service.OtterPocketGrpcServiceGrpc.OtterPocketGrpcServiceBlockingStub;
import responsive.otterpocket.v1.service.OtterPocketGrpcServiceGrpc.OtterPocketGrpcServiceStub;
import responsive.otterpocket.v1.service.Otterpockets.WriteWALSegmentRequest;
import responsive.otterpocket.v1.service.Otterpockets.WriteWALSegmentResult;

public class OtterPocketGrpcClient<QT extends Message, CT extends Message>
    implements OtterPocketClient<QT, CT> {
  private final ManagedChannel channel;
  private final OtterPocketGrpcServiceBlockingStub stub;
  private final OtterPocketGrpcServiceStub nbStub;

  public OtterPocketGrpcClient(final String target) {
    final ChannelCredentials credentials = InsecureChannelCredentials.create();
    channel = Grpc.newChannelBuilder(target, credentials).build();
    stub = OtterPocketGrpcServiceGrpc.newBlockingStub(channel);
    nbStub = OtterPocketGrpcServiceGrpc.newStub(channel);
  }

  OtterPocketGrpcClient(final ManagedChannel channel,
                        final OtterPocketGrpcServiceBlockingStub stub,
                        final OtterPocketGrpcServiceStub nbStub) {
    this.channel = Objects.requireNonNull(channel);
    this.stub = Objects.requireNonNull(stub);
    this.nbStub = Objects.requireNonNull(nbStub);
  }

  @Override
  public byte[] query(final int lssId, QT query) {
    final var result = stub.query(QueryRequest.newBuilder()
        .setLssId(lssId)
        .setQuery(query.toByteString())
        .build());
    return result.hasData() ? result.getData().toByteArray() : null;
  }

  @Override
  public GrpcWALSegmentWriter<CT> writeSegment(
      final int lssId,
      final long endOffset,
      final long currentEndOffset
  ) {
    final var responseObserver = new BlockingResponseObserver<WriteWALSegmentResult>();
    final var requestObserver = nbStub.writeWALSegment(responseObserver);
    return new GrpcWALSegmentWriter<>(
        endOffset,
        currentEndOffset,
        lssId,
        requestObserver,
        responseObserver
    );
  }

  private static class BlockingResponseObserver<T> implements StreamObserver<T> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private T value;
    private Throwable error;

    @Override
    public void onNext(final T value) {
      this.value = value;
    }

    @Override
    public void onError(final Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {
      latch.countDown();
    }

    public T get() {
      try {
        latch.await();
      } catch(final InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (error != null) {
        if (error instanceof RuntimeException) {
          throw (RuntimeException) error;
        }
        throw new RuntimeException(error);
      }
      return value;
    }
  }

  public class GrpcWALSegmentWriter<CT extends Message> implements WALSegmentWriter<CT> {
    private final long endOffset;
    private final long currentEndOffset;
    private final int lssId;
    private final StreamObserver<WriteWALSegmentRequest> requestObserver;
    private final BlockingResponseObserver<WriteWALSegmentResult> resultObserver;

    public GrpcWALSegmentWriter(
        final long endOffset,
        final long currentEndOffset,
        final int lssId,
        final StreamObserver<WriteWALSegmentRequest> requestObserver,
        final BlockingResponseObserver<WriteWALSegmentResult> resultObserver) {
      this.endOffset = endOffset;
      this.currentEndOffset = currentEndOffset;
      this.lssId = lssId;
      this.requestObserver = Objects.requireNonNull(requestObserver);
      this.resultObserver = Objects.requireNonNull(resultObserver);
    }

    public void write(CT change) {
      requestObserver.onNext(WriteWALSegmentRequest.newBuilder()
          .setLssId(lssId)
          .setCurrentEndOffset(currentEndOffset)
          .setEndOffset(endOffset)
          .setData(change.toByteString())
          .build());
    }

    public void close() {
      requestObserver.onCompleted();
      resultObserver.get();
    }
  }
}
