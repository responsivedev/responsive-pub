package dev.responsive.kafka.internal.db.rs3.client;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class CreateStoreTypes {

  public enum ClockType {
    WALL_CLOCK,
    STREAM_TIME
  }

  public enum StoreType {
    BASIC,
    WINDOW
  }

  public static class CreateStoreOptions {

    private final int logicalShards;
    private final StoreType storeType;
    private final Optional<ClockType> clockType;
    private final Optional<Long> defaultTtl;
    private final Optional<Integer> filterBitsPerKey;

    public CreateStoreOptions(
        final int logicalShards,
        final StoreType storeType,
        final Optional<ClockType> clockType,
        final Optional<Long> defaultTtl,
        final Optional<Integer> filterBitsPerKey
    ) {
      this.logicalShards = logicalShards;
      this.storeType = storeType;
      this.clockType = clockType;
      this.defaultTtl = defaultTtl;
      this.filterBitsPerKey = filterBitsPerKey;
    }

    public int logicalShards() {
      return logicalShards;
    }

    public StoreType storeType() {
      return storeType;
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

    @Override
    public String toString() {
      return "CreateStoreOptions{"
          + "logicalShards=" + logicalShards
          + ", storeType=" + storeType
          + ", clockType=" + clockType
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
      return logicalShards == that.logicalShards && storeType == that.storeType
          && Objects.equals(clockType, that.clockType) && Objects.equals(
          defaultTtl,
          that.defaultTtl
      ) && Objects.equals(filterBitsPerKey, that.filterBitsPerKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(logicalShards, storeType, clockType, defaultTtl, filterBitsPerKey);
    }
  }

  public static class CreateStoreResult {
    private final UUID storeId;
    private final List<Integer> pssIds;

    public CreateStoreResult(final UUID storeId, final List<Integer> pssIds) {
      this.storeId = storeId;
      this.pssIds = pssIds;
    }

    public UUID storeId() {
      return storeId;
    }

    public List<Integer> pssIds() {
      return pssIds;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final CreateStoreResult that = (CreateStoreResult) o;

      if (!storeId.equals(that.storeId)) {
        return false;
      }
      return pssIds.equals(that.pssIds);
    }

    @Override
    public int hashCode() {
      int result = storeId.hashCode();
      result = 31 * result + pssIds.hashCode();
      return result;
    }
  }

}
