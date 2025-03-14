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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

class GrpcStreamSender<M, P> implements StreamSender<M> {
  private final Function<M, P> protoFactory;
  private final StreamObserver<P> grpcObserver;
  private final CompletableFuture<Void> future = new CompletableFuture<>();

  GrpcStreamSender(
      final Function<M, P> protoFactory,
      final StreamObserver<P> grpcObserver) {
    this.protoFactory = Objects.requireNonNull(protoFactory);
    this.grpcObserver = Objects.requireNonNull(grpcObserver);
  }

  @Override
  public void sendNext(M msg) {
    try {
      if (isActive()) {
        grpcObserver.onNext(protoFactory.apply(msg));
      }
    } catch (Exception e) {
      fail(e);
    }
  }

  @Override
  public void finish() {
    try {
      if (isActive()) {
        grpcObserver.onCompleted();
        future.complete(null);
      }
    } catch (Exception e) {
      fail(e);
    }
  }

  @Override
  public void cancel() {
    fail(new RuntimeException("message stream cancelled"));
  }

  private void fail(Exception e) {
    grpcObserver.onError(e);
    final var publicError = GrpcRs3Util.wrapThrowable(e);
    future.completeExceptionally(publicError);
  }

  public boolean isActive() {
    return !future.isDone();
  }

  @Override
  public CompletionStage<Void> completion() {
    return future;
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

}
