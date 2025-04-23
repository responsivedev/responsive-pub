package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Arrays;
import java.util.Objects;

public class Delete extends WalEntry {
  private final byte[] key;

  public Delete(final byte[] key) {
    this.key = Objects.requireNonNull(key);
  }

  public byte[] key() {
    return key;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Delete delete = (Delete) o;
    return Objects.deepEquals(key, delete.key);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }
}
