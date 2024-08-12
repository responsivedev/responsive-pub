package dev.responsive.kafka.internal.db.pocket.client;

// exception types for interesting failures like unaligned wal segment of read offset check fail
public class PocketException extends RuntimeException {
  private static final long serialVersionUID = 0L;

  public PocketException(final Throwable cause) {
    super(cause);
  }
}
