package dev.responsive.kafka.internal.db.rs3.client;

import dev.responsive.rs3.Rs3;
import java.util.Optional;

public class CreateStoreOptions {

  public enum ClockType {
    WALL_CLOCK,
    STREAM_TIME
  }

  private final Optional<ClockType> clockType;
  private final Optional<Long> defaultTtl;
  private final Optional<Integer> filterBitsPerKey;


  public CreateStoreOptions(
      final Optional<ClockType> clockType,
      final Optional<Long> defaultTtl,
      final Optional<Integer> filterBitsPerKey
  ) {
    this.clockType = clockType;
    this.defaultTtl = defaultTtl;
    this.filterBitsPerKey = filterBitsPerKey;
  }

  public Optional<ClockType> clockType() {
    return clockType;
  }

  public Optional<Long> defaultTtl() {
    return defaultTtl;
  }

  public Optional<Integer> filterBitsPerKey() {
    return filterBitsPerKey;
  }

  public Rs3.CreateStoreOptions toProto() {
    final var builder = Rs3.CreateStoreOptions.newBuilder();
    clockType.ifPresent(
        type -> builder.setClockType(Rs3.CreateStoreOptions.ClockType.forNumber(type.ordinal()))
    );
    defaultTtl.ifPresent(builder::setDefaultTtl);
    filterBitsPerKey.ifPresent(builder::setFilterBitsPerKey);
    return builder.build();
  }

  @Override
  public String toString() {
    return "CreateStoreOptions{"
        + "clockType=" + clockType
        + ", defaultTtl=" + defaultTtl
        + ", filterBitsPerKey=" + filterBitsPerKey
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CreateStoreOptions that = (CreateStoreOptions) o;

    if (!clockType.equals(that.clockType)) {
      return false;
    }
    if (!defaultTtl.equals(that.defaultTtl)) {
      return false;
    }
    return filterBitsPerKey.equals(that.filterBitsPerKey);
  }

  @Override
  public int hashCode() {
    int result = clockType.hashCode();
    result = 31 * result + defaultTtl.hashCode();
    result = 31 * result + filterBitsPerKey.hashCode();
    return result;
  }
}
