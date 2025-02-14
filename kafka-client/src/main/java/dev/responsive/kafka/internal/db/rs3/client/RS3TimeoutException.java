package dev.responsive.kafka.internal.db.rs3.client;

public class RS3TimeoutException extends RS3Exception {
  private static final long serialVersionUID = 0L;

  public RS3TimeoutException(final Throwable cause) {
    super(cause);
  }

  public RS3TimeoutException(final String message) {
    super(message);
  }

}
