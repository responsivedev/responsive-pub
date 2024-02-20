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

import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class WindowedTableCache<T extends RemoteTable<?, ?>> {

  @FunctionalInterface
  public interface Factory<T> {
    T create(final CassandraTableSpec spec, WindowSegmentPartitioner partitioner)
        throws InterruptedException, TimeoutException;
  }

  private final Map<String, T> tables = new HashMap<>();
  private final Factory<T> factory;

  public WindowedTableCache(final Factory<T> factory) {
    this.factory = factory;
  }

  /**
   * Creates a table with the supplied {@code tableName} with the
   * desired schema.
   */
  public synchronized T create(CassandraTableSpec spec, WindowSegmentPartitioner partitioner)
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
   *         {@link #create(CassandraTableSpec, WindowSegmentPartitioner)}
   */
  public T getTable(final String name) {
    return tables.get(name);
  }

}
