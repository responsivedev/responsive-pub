package dev.responsive.kafka.internal.db.rs3.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class GrpcRs3UtilTest {

  @Test
  public void shouldReturnTransientExceptionForUnavailableErrors() {
    final var exception = new StatusRuntimeException(Status.UNAVAILABLE);
    final var wrapped = GrpcRs3Util.wrapThrowable(exception);
    assertThat(wrapped, instanceOf(RS3TransientException.class));
  }

  @Test
  public void shouldReturnTransientExceptionForResourceExhaustedErrors() {
    final var exception = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
    final var wrapped = GrpcRs3Util.wrapThrowable(exception);
    assertThat(wrapped, instanceOf(RS3TransientException.class));
  }

}