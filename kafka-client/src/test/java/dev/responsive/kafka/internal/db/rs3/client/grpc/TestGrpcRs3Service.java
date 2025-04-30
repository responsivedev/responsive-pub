package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.rs3.RS3Grpc;
import dev.responsive.rs3.Rs3;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class TestGrpcRs3Service extends RS3Grpc.RS3ImplBase {

  interface KeyValueStore {
    void put(Rs3.KeyValue kv);

    void delete(Rs3.Key key);

    Optional<Rs3.KeyValue> get(Rs3.Key key);

    Stream<Rs3.KeyValue> range(Rs3.Range range);
  }

  private final UUID storeId;
  private final LssId lssId;
  private final int pssId;

  private final AtomicLong offset = new AtomicLong(0);
  private final KeyValueStore store;

  public TestGrpcRs3Service(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final KeyValueStore store
  ) {
    this.storeId = storeId;
    this.lssId = lssId;
    this.pssId = pssId;
    this.store = store;
  }

  @Override
  public void getOffsets(
      final Rs3.GetOffsetsRequest req,
      final StreamObserver<Rs3.GetOffsetsResult> responseObserver
  ) {
    final var storeId = new UUID(
        req.getStoreId().getHigh(),
        req.getStoreId().getLow()
    );
    if (req.getPssId() != pssId
        || req.getLssId().getId() != lssId.id()
        || !storeId.equals(this.storeId)) {
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
    }

    final var currentOffset = offset.get();
    final var result = Rs3.GetOffsetsResult
        .newBuilder()
        .setFlushedOffset(GrpcRs3Util.walOffsetProto(currentOffset))
        .setWrittenOffset(GrpcRs3Util.walOffsetProto(currentOffset))
        .build();
    responseObserver.onNext(result);
    responseObserver.onCompleted();
  }

  @Override
  public void get(
      final Rs3.GetRequest req,
      final StreamObserver<Rs3.GetResult> responseObserver
  ) {
    final var storeId = new UUID(
        req.getStoreId().getHigh(),
        req.getStoreId().getLow()
    );
    if (req.getPssId() != pssId
        || req.getLssId().getId() != lssId.id()
        || !storeId.equals(this.storeId)) {
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
      return;
    }

    if (req.getExpectedMinWrittenOffset().getIsWritten()) {
      if (offset.get() < req.getExpectedMinWrittenOffset().getOffset()) {
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }
    }

    final var resultBldr = Rs3.GetResult.newBuilder();
    final var kv = store.get(req.getKey());
    kv.ifPresent(resultBldr::setResult);
    responseObserver.onNext(resultBldr.build());
    responseObserver.onCompleted();
  }

  @Override
  public void range(
      final Rs3.RangeRequest req,
      final StreamObserver<Rs3.RangeResult> responseObserver
  ) {
    final var storeId = new UUID(
        req.getStoreId().getHigh(),
        req.getStoreId().getLow()
    );
    if (req.getPssId() != pssId
        || req.getLssId().getId() != lssId.id()
        || !storeId.equals(this.storeId)) {
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
      return;
    }

    if (req.getExpectedMinWrittenOffset().getIsWritten()) {
      if (offset.get() < req.getExpectedMinWrittenOffset().getOffset()) {
        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        return;
      }
    }

    store.range(req.getRange()).forEach(kv -> {
      final var keyValueResult = Rs3.RangeResult.newBuilder()
          .setType(Rs3.RangeResult.Type.RESULT)
          .setResult(kv)
          .build();
      responseObserver.onNext(keyValueResult);
    });

    final var endOfStream = Rs3.RangeResult.newBuilder()
        .setType(Rs3.RangeResult.Type.END_OF_STREAM)
        .build();
    responseObserver.onNext(endOfStream);
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<Rs3.WriteWALSegmentRequest> writeWALSegmentStream(
      final StreamObserver<Rs3.WriteWALSegmentResult> responseObserver
  ) {
    return new StreamObserver<>() {
      @Override
      public void onNext(final Rs3.WriteWALSegmentRequest req) {
        final var storeId = new UUID(
            req.getStoreId().getHigh(),
            req.getStoreId().getLow()
        );
        if (req.getPssId() != pssId
            || req.getLssId().getId() != lssId.id()
            || !storeId.equals(TestGrpcRs3Service.this.storeId)) {
          responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
        }

        if (req.getExpectedWrittenOffset().getIsWritten()) {
          if (offset.get() < req.getExpectedWrittenOffset().getOffset()) {
            responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
            return;
          }
        }

        TestGrpcRs3Service.this.offset.getAndUpdate(
            current -> Math.max(current, req.getEndOffset())
        );
        if (req.hasPut()) {
          store.put(req.getPut().getKv());
        } else if (req.hasDelete()) {
          store.delete(req.getDelete().getKey());
        }
      }

      @Override
      public void onError(final Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        final var result = Rs3.WriteWALSegmentResult
            .newBuilder()
            .setFlushedOffset(GrpcRs3Util.walOffsetProto(offset.get()))
            .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
      }
    };
  }
}
