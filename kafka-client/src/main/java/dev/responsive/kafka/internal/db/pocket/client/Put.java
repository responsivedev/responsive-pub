package dev.responsive.kafka.internal.db.pocket.client;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class Put extends WalEntry {
  private final byte[] key;
  private final byte[] value;

  public Put(final byte[] key, final byte[] value) {
    this.key = Objects.requireNonNull(key);
    this.value = value;
  }

  public byte[] key() {
    return key;
  }

  public Optional<byte[]> value() {
    return Optional.ofNullable(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Put)) {
      return false;
    }
    final Put put = (Put) o;
    return Objects.deepEquals(key, put.key) && Objects.deepEquals(value, put.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
  }
}
