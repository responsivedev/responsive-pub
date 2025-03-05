package dev.responsive.kafka.internal.db.rs3.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RS3RetryUtilTest {

  private final MockTime time = new MockTime();

  @Mock
  private Supplier<Integer> operation;


  @Test
  public void shouldRetryOperation() {
    when(operation.get())
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenReturn(1);

    var retryTimeoutMs = 30000L;
    var retryUtil = new RS3RetryUtil(retryTimeoutMs, time);
    assertThat(retryUtil.withRetry(operation, () -> "test_operation"), is(1));
  }

  @Test
  public void shouldTimeoutOperation() {
    when(operation.get())
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

    var retryTimeoutMs = 30000L;
    var retryUtil = new RS3RetryUtil(retryTimeoutMs, time);
    var startTimeMs = time.milliseconds();

    assertThrows(
        RS3TimeoutException.class,
        () -> retryUtil.withRetry(operation, () -> "test_operation")
    );
    assertThat(time.milliseconds() - startTimeMs, is(retryTimeoutMs));
  }

  @Test
  public void shouldPropagateUnexpectedException() {
    when(operation.get())
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN));

    var retryTimeoutMs = 30000L;
    var retryUtil = new RS3RetryUtil(retryTimeoutMs, time);
    RS3Exception e = assertThrows(
        RS3Exception.class,
        () -> retryUtil.withRetry(operation, () -> "test_operation")
    );
    assertThat(e.getCause(), is(instanceOf(StatusRuntimeException.class)));
    assertThat(((StatusRuntimeException) e.getCause()).getStatus(), is(Status.UNKNOWN));
  }

}