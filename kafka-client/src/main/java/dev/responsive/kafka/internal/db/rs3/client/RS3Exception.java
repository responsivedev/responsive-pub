package dev.responsive.kafka.internal.db.rs3.client;

// exception types for interesting failures like unaligned wal segment of read offset check fail
public class RS3Exception extends RuntimeException {
  private static final long serialVersionUID = 0L;

  public RS3Exception(final Throwable cause) {
    super(cause);
  }
}
