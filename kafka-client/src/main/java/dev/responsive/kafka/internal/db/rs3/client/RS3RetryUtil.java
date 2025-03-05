package dev.responsive.kafka.internal.db.rs3.client;

import static io.grpc.Status.UNAVAILABLE;

import io.grpc.StatusRuntimeException;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;

public class RS3RetryUtil {

  public static ExponentialBackoff defaultBackoff() {
    // Using Kafka default backoff settings initially. We can pull them up
    // if there is ever strong reason.
    return new ExponentialBackoff(50, 2, 1000, 0.2);
  }

  private final Time time;
  private final long retryTimeoutMs;

  public RS3RetryUtil(
      long retryTimeoutMs,
      Time time
  ) {
    this.retryTimeoutMs = retryTimeoutMs;
    this.time = time;
  }

  public <T> T withRetryFallback(
      Supplier<T> operation,
      Supplier<T> fallbackOperation,
      Supplier<String> describeOperation
  ) {
    // Using Kafka default backoff settings initially. We can pull them up
    // if there is ever strong reason.
    final var backoff = defaultBackoff();
    final var startTimeMs = time.milliseconds();
    final var deadlineMs = startTimeMs + retryTimeoutMs;

    var retries = 0;
    long currentTimeMs;

    do {
      try {
        if (retries == 0) {
          return operation.get();
        } else{
          return fallbackOperation.get();
        }
      } catch (final StatusRuntimeException e) {
        if (e.getStatus() != UNAVAILABLE) {
          throw new RS3Exception(e);
        }
      }

      retries += 1;
      currentTimeMs = time.milliseconds();
      time.sleep(Math.min(
          backoff.backoff(retries),
          Math.max(0, deadlineMs - currentTimeMs))
      );
    } while (currentTimeMs - startTimeMs < retryTimeoutMs);

    throw new RS3TimeoutException("Timeout while attempting operation " + describeOperation.get());
  }

  public <T> T withRetry(Supplier<T> operation, Supplier<String> describeOperation) {
    return withRetryFallback(
        operation,
        operation,
        describeOperation
    );
  }

}
