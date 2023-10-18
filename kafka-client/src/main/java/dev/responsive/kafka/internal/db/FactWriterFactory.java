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
import dev.responsive.kafka.internal.utils.SessionClients;

public class FactWriterFactory<K> implements WriterFactory<K> {

  private final RemoteTable<K, BoundStatement> table;

  public FactWriterFactory(final RemoteTable<K, BoundStatement> table) {
    this.table = table;
  }

  @Override
  public RemoteWriter<K> createWriter(
      final SessionClients client,
      final int partition,
      final int batchSize
  ) {
    return new FactSchemaWriter<>(
        client.cassandraClient(),
        table,
        partition
    );
  }

  @Override
  public String toString() {
    return "FactWriterFactory{}";
  }
}
