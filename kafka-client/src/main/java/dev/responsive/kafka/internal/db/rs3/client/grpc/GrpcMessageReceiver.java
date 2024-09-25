package dev.responsive.kafka.internal.db.rs3.client.grpc;

import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class GrpcMessageReceiver<T> implements StreamObserver<T> {
  private T result = null;
  private RuntimeException error = null;
  private final CompletableFuture<T> future = new CompletableFuture<>();

  @Override
  synchronized public void onNext(T result) {
    if (this.result == null) {
      this.result = result;
    } else {
      onError(new IllegalStateException("onNext called on result observer with existing result"));
    }
  }

  @Override
  synchronized public void onError(final Throwable throwable) {
    if (throwable instanceof StatusRuntimeException || throwable instanceof StatusException) {
      error = new RS3Exception(throwable);
    } else if (throwable instanceof RuntimeException) {
      error = (RuntimeException) throwable;
    } else {
      error = new RuntimeException(throwable);
    }
    complete();
  }

  @Override
  synchronized public void onCompleted() {
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
