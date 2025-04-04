/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.SessionSegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SessionTableCache<T extends RemoteTable<?, ?>> {

  @FunctionalInterface
  public interface Factory<T> {
    T create(final RemoteTableSpec spec, SessionSegmentPartitioner partitioner)
        throws InterruptedException, TimeoutException;
  }

  private final Map<String, T> tables = new HashMap<>();
  private final Factory<T> factory;

  public SessionTableCache(final Factory<T> factory) {
    this.factory = factory;
  }

  /**
   * Creates a table with the supplied {@code tableName} with the
   * desired schema.
   */
  public synchronized T create(RemoteTableSpec spec, SessionSegmentPartitioner partitioner)
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
   *         {@link #create(RemoteTableSpec, SessionSegmentPartitioner)}
   */
  public synchronized T getTable(final String name) {
    return tables.get(name);
  }

}
