package dev.responsive.kafka.internal.db.rs3.client;

import java.util.List;
import java.util.UUID;

public class Store {

  private final String storeName;
  private final UUID storeId;
  private final List<Integer> pssIds;

  public Store(final String storeName, final UUID storeId, final List<Integer> pssIds) {
    this.storeName = storeName;
    this.storeId = storeId;
    this.pssIds = List.copyOf(pssIds);
  }

  public String storeName() {
    return storeName;
  }

  public UUID storeId() {
    return storeId;
  }

  public List<Integer> pssIds() {
    return pssIds;
  }

  @Override
  public String toString() {
    return "Store{" +
        "storeName='" + storeName + '\'' +
        ", storeId=" + storeId +
        ", pssIds=" + pssIds +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Store store = (Store) o;

    if (!storeName.equals(store.storeName)) {
      return false;
    }
    if (!storeId.equals(store.storeId)) {
      return false;
    }
    return pssIds.equals(store.pssIds);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + storeId.hashCode();
    result = 31 * result + pssIds.hashCode();
    return result;
  }
}
