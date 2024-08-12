package dev.responsive.kafka.internal.db.pocket.client.grpc;

import dev.responsive.kafka.internal.db.pocket.client.StreamSender;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.Function;

class GrpcStreamSender<M, P> implements StreamSender<M> {
  private final Function<M, P> protoFactory;
  private final StreamObserver<P> grpcObserver;

  GrpcStreamSender(
      final Function<M, P> protoFactory,
      final StreamObserver<P> grpcObserver) {
    this.protoFactory = Objects.requireNonNull(protoFactory);
    this.grpcObserver = Objects.requireNonNull(grpcObserver);
  }

  @Override
  public void sendNext(M msg) {
    grpcObserver.onNext(protoFactory.apply(msg));
  }

  @Override
  public void finish() {
    grpcObserver.onCompleted();
  }

  @Override
  public void cancel() {
    grpcObserver.onError(new RuntimeException("message stream cancelled"));
  }
}
