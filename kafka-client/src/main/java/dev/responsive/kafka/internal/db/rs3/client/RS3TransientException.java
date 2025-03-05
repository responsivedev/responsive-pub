package dev.responsive.kafka.internal.db.rs3.client;

public class RS3TransientException extends RS3Exception {
  private static final long serialVersionUID = 0L;

  public RS3TransientException(final Throwable cause) {
    super(cause);
  }
}
