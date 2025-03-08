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

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class GrpcMessageReceiver<T> implements StreamObserver<T> {
  private T result = null;
  private RuntimeException error = null;
  private final CompletableFuture<T> future = new CompletableFuture<>();

  @Override
  public synchronized void onNext(T result) {
    if (this.result == null) {
      this.result = result;
    } else {
      onError(new IllegalStateException("onNext called on result observer with existing result"));
    }
  }

  @Override
  public synchronized void onError(final Throwable throwable) {
    error = GrpcRs3Util.wrapThrowable(throwable);
    complete();
  }

  @Override
  public synchronized void onCompleted() {
    complete();
  }

  CompletionStage<T> message() {
    return future;
  }

  private void complete() {
    if (error != null) {
      future.completeExceptionally(error);
      return;
    }
    if (result == null) {
      future.completeExceptionally(
          new IllegalStateException("result observer completed with no result or error"));
    }
    future.complete(result);
  }
}
