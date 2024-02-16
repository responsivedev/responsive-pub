package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class WindowedTableCache<T extends RemoteTable<?, ?>> {
  @FunctionalInterface
  public interface Factory<T> {
    T create(final CassandraTableSpec spec, SegmentPartitioner<WindowedKey> partitioner)
        throws InterruptedException, TimeoutException;
  }

  private final Map<String, T> tables = new HashMap<>();
  private final Factory<T> factory;

  public WindowedTableCache(final Factory<T> factory) { this.factory = factory; }

  /**
   * Creates a table with the supplied {@code tableName} with the
   * desired schema.
   */
  public synchronized T create(CassandraTableSpec spec, SegmentPartitioner<WindowedKey> partitioner)
      throws InterruptedException, TimeoutException {
    final T existing = tables.get(spec.tableName());
    if (existing != null) {
      return existing;
    }

    final T table = factory.create(spec, partitioner);
    tables.put(spec.tableName(), table);
    return table;
  }

  /**
   * @param name the name of the table
   * @return the table, if it was already created by
   *         {@link #create(CassandraTableSpec, SegmentPartitioner)}
   */
  public T getTable(final String name) {
    return tables.get(name);
  }

}
