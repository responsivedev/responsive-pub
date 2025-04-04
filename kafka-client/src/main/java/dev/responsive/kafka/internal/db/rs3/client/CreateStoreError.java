package dev.responsive.kafka.internal.db.rs3.client;

public enum CreateStoreError {
  FATAL,      // 0
  RETRIABLE;   // 1


  public static CreateStoreError fromCode(final int errorCode) {
    final CreateStoreError[] values = CreateStoreError.values();
    if (errorCode < 0 || errorCode >= values.length) {
      throw new IllegalArgumentException("Invalid error code for CreateStoreError: " + errorCode);
    }
    return values[errorCode];
  }

}

