package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpsRs3TestUtil.newEndOfStreamResult;
import static dev.responsive.kafka.internal.db.rs3.client.grpc.GrpsRs3TestUtil.newKeyValueResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import dev.responsive.kafka.internal.db.rs3.client.RS3Exception;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.Rs3;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcKeyValueIteratorTest {

  @Mock
  private GrpcRangeRequestProxy requestProxy;

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIterateKeyValueResults() {
    final var startBound = RangeBound.inclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("a"));
      observer.onNext(newKeyValueResult("b"));
      observer.onNext(newKeyValueResult("c"));
      observer.onNext(newEndOfStreamResult());
      observer.onCompleted();
      return null;
    }).when(requestProxy).send(eq(startBound), any());

    try (final var iter = new GrpcKeyValueIterator(startBound, requestProxy)) {
      assertNextKey(iter, "a");
      assertNextKey(iter, "b");
      assertNextKey(iter, "c");
      assertThat(iter.hasNext(), is(false));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryRangeRequestAfterTransientFailure() {
    final var startBound = RangeBound.inclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("a"));
      observer.onError(new StatusRuntimeException(Status.UNAVAILABLE));
      return null;
    }).when(requestProxy).send(eq(startBound), any());

    final var retryStartBound = RangeBound.exclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("b"));
      observer.onNext(newKeyValueResult("c"));
      observer.onNext(newEndOfStreamResult());
      observer.onCompleted();
      return null;
    }).when(requestProxy).send(eq(retryStartBound), any());

    try (final var iter = new GrpcKeyValueIterator(startBound, requestProxy)) {
      assertNextKey(iter, "a");
      assertNextKey(iter, "b");
      assertNextKey(iter, "c");
      assertThat(iter.hasNext(), is(false));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryAfterUnexpectedStreamCompletion() {
    final var startBound = RangeBound.inclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("a"));
      observer.onCompleted();
      return null;
    }).when(requestProxy).send(eq(startBound), any());

    final var retryStartBound = RangeBound.exclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("b"));
      observer.onNext(newKeyValueResult("c"));
      observer.onNext(newEndOfStreamResult());
      observer.onCompleted();
      return null;
    }).when(requestProxy).send(eq(retryStartBound), any());

    try (final var iter = new GrpcKeyValueIterator(startBound, requestProxy)) {
      assertNextKey(iter, "a");
      assertNextKey(iter, "b");
      assertNextKey(iter, "c");
      assertThat(iter.hasNext(), is(false));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldPropagateUnexpectedFailures() {
    final var startBound = RangeBound.inclusive("a".getBytes(StandardCharsets.UTF_8));
    Mockito.doAnswer(invocation -> {
      StreamObserver<Rs3.RangeResult> observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(newKeyValueResult("a"));
      observer.onError(new StatusRuntimeException(Status.UNKNOWN));
      return null;
    }).when(requestProxy).send(eq(startBound), any());

    try (final var iter = new GrpcKeyValueIterator(startBound, requestProxy)) {
      assertNextKey(iter, "a");
      final var rs3Exception = assertThrows(RS3Exception.class, iter::next);
      assertThat(rs3Exception.getCause(), instanceOf(StatusRuntimeException.class));
      final var statusException = (StatusRuntimeException) rs3Exception.getCause();
      assertThat(statusException.getStatus().getCode(), is(Status.Code.UNKNOWN));
    }
  }

  private void assertNextKey(GrpcKeyValueIterator iter, String key) {
    assertThat(iter.hasNext(), is(true));
    final var keyValue = iter.next();
    final var keyBytes = keyValue.key.get();
    final var keyString = new String(keyBytes, StandardCharsets.UTF_8);
    assertThat(keyString, is(key));
  }


}