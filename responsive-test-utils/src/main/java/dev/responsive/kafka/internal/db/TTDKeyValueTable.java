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

import dev.responsive.kafka.internal.clients.TTDCassandraClient;
import dev.responsive.kafka.internal.db.inmemory.InMemoryKVTable;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDKeyValueTable extends InMemoryKVTable {

  private final TTDCassandraClient client;

  public TTDKeyValueTable(final RemoteTableSpec spec, final TTDCassandraClient client) {
    super(spec.tableName(), spec.ttlResolver());
    this.client = client;
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long streamTimeMs) {
    client.flush();

    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    return super.get(kafkaPartition, key, streamTimeMs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    client.flush();

    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    return super.range(kafkaPartition, from, to, streamTimeMs);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long streamTimeMs) {
    client.flush();

    // trigger flush before lookup since CommitBuffer doesn't apply ttl
    return super.all(kafkaPartition, streamTimeMs);
  }

}