package dev.responsive.kafka.internal.db.rs3.client;

import java.util.List;
import java.util.UUID;

public class Table {

  private final UUID storeId;
  private final List<Integer> pssIds;

  public Table(final UUID storeId, final List<Integer> pssIds) {
    this.storeId = storeId;
    this.pssIds = pssIds;
  }

  public UUID storeId() {
    return storeId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Table table = (Table) o;

    if (!storeId.equals(table.storeId)) {
      return false;
    }
    return pssIds.equals(table.pssIds);
  }

  @Override
  public int hashCode() {
    int result = storeId.hashCode();
    result = 31 * result + pssIds.hashCode();
    return result;
  }
}
