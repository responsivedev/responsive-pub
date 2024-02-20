/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@code ThreadSafeTableFactory} creates and maintains a collection
 * of {@link RemoteTable}s, ensuring that the statements to create the
 * table are only prepared once during the lifetime of the application.
 */
@ThreadSafe
public class TableCache<T extends RemoteTable<?, ?>> {

  @FunctionalInterface
  public interface Factory<T> {
    T create(final CassandraTableSpec spec)
        throws InterruptedException, TimeoutException;
  }

  private final Map<String, T> tables = new HashMap<>();
  private final Factory<T> factory;

  public TableCache(final Factory<T> factory) {
    this.factory = factory;
  }

  /**
   * Creates a table with the supplied {@code tableName} with the
   * desired schema.
   */
  public synchronized T create(CassandraTableSpec spec)
      throws InterruptedException, TimeoutException {
    final T existing = tables.get(spec.tableName());
    if (existing != null) {
      return existing;
    }

    final T table = factory.create(spec);
    tables.put(spec.tableName(), table);
    return table;
  }

  /**
   * @param name the name of the table
   * @return the table, if it was already created by {@link #create(CassandraTableSpec)}
   */
  public synchronized T getTable(final String name) {
    return tables.get(name);
  }

}
