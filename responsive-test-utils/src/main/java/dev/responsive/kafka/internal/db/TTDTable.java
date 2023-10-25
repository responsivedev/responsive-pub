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
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Time;

public abstract class TTDTable<K> implements RemoteTable<K, BoundStatement> {

  protected final TTDCassandraClient client;
  protected final Time time;

  public TTDTable(final TTDCassandraClient client) {
    this.client = client;
    this.time = client.time();
  }

  /**
   * @return the number of elements in this table
   *         or 0 if the schema has no such table
   */
  public abstract long count();

  @SuppressWarnings("unchecked") // see comment on RemoteTable#init
  @Override
  public WriterFactory<K, Integer> init(
      final int kafkaPartition
  ) {
    return new TTDWriterFactory<>(this, kafkaPartition);
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return 0;
  }

  @Override
  public BoundStatement setOffset(final int kafkaPartition, final long offset) {
    return null;
  }

}
