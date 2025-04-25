package dev.responsive.kafka.internal.db.rs3.client;

import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.StoreType;
import java.util.List;
import java.util.UUID;

public class StoreInfo {

  public enum Status {
    CREATING,
    READY,
    DELETED
  }

  private final String storeName;
  private final UUID storeId;
  private final StoreType storeType;
  private final Status status;
  private final List<Integer> pssIds;
  private final String createOptions;

  public StoreInfo(
      final String storeName,
      final UUID storeId,
      final StoreType storeType,
      final Status status,
      final List<Integer> pssIds,
      final String createOptions
  ) {
    this.storeName = storeName;
    this.storeId = storeId;
    this.storeType = storeType;
    this.status = status;
    this.pssIds = List.copyOf(pssIds);
    this.createOptions = createOptions;
  }

  public String storeName() {
    return storeName;
  }

  public UUID storeId() {
    return storeId;
  }

  public StoreType storeType() {
    return storeType;
  }

  public Status status() {
    return status;
  }

  public List<Integer> pssIds() {
    return pssIds;
  }

  public String createOptions() {
    return createOptions;
  }

  @Override
  public String toString() {
    return "StoreInfo{"
        + "storeName='" + storeName + '\''
        + ", storeId=" + storeId
        + ", storeType=" + storeType
        + ", status=" + status
        + ", pssIds=" + pssIds
        + ", createOptions=" + createOptions
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

    final StoreInfo storeInfo = (StoreInfo) o;

    if (!storeName.equals(storeInfo.storeName)) {
      return false;
    }
    if (!storeId.equals(storeInfo.storeId)) {
      return false;
    }
    if (!storeType.equals(storeInfo.storeType)) {
      return false;
    }
    if (!status.equals(storeInfo.status)) {
      return false;
    }
    if (!pssIds.equals(storeInfo.pssIds)) {
      return false;
    }
    return createOptions.equals(storeInfo.createOptions);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + storeId.hashCode();
    result = 31 * result + storeType.hashCode();
    result = 31 * result + status.hashCode();
    result = 31 * result + pssIds.hashCode();
    result = 31 * result + createOptions.hashCode();
    return result;
  }
}
