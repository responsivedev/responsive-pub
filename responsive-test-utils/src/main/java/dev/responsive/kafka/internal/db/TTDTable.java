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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.clients.TTDCassandraClient;

// TODO: use mock return values instead of null here and in the KV/Window child classes
public abstract class TTDTable<K> implements RemoteTable<K, BoundStatement> {

  protected final TTDCassandraClient client;

  public TTDTable(final TTDCassandraClient client) {
    this.client = client;
  }

  /**
   * @return the number of elements in this table
   *         or 0 if the schema has no such table
   */
  public abstract long count();

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return 0;
  }

}
