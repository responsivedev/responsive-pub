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

import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
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
