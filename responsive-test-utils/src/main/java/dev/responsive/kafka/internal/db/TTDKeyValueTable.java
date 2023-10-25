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
import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;
import dev.responsive.kafka.internal.db.spec.DelegatingTableSpec;
import dev.responsive.kafka.internal.db.spec.TtlTableSpec;
import dev.responsive.kafka.internal.stores.KVStoreStub;
import java.time.Duration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDKeyValueTable extends TTDTable<Bytes> implements RemoteKVTable<BoundStatement> {

  private final String name;
  private final KVStoreStub stub;

  public static TTDKeyValueTable create(
      final CassandraTableSpec spec,
      final CassandraClient client
  ) {
    return new TTDKeyValueTable(spec, (TTDCassandraClient) client);
  }

  public TTDKeyValueTable(final CassandraTableSpec spec, final TTDCassandraClient client) {
    super(client);

    name = spec.tableName();
    Duration ttl = null;
    CassandraTableSpec maybeTtlSpec = spec;

    while (maybeTtlSpec instanceof DelegatingTableSpec) {
      if (maybeTtlSpec instanceof TtlTableSpec) {
        ttl = ((TtlTableSpec) maybeTtlSpec).ttl();
        break;
      }

      maybeTtlSpec = ((DelegatingTableSpec) maybeTtlSpec).delegate();
    }

    stub = new KVStoreStub(ttl, time);
  }

  @Override
  public long count() {
    return stub.count();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public BoundStatement insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    stub.put(key, value);
    return null;
  }

  @Override
  public BoundStatement delete(final int kafkaPartition, final Bytes key) {
    stub.delete(key);
    return null;
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, long minValidTs) {
    return stub.get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      Bytes from,
      final Bytes to,
      long minValidTs
  ) {
    return stub.range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(
      final int kafkaPartition,
      long minValidTs
  ) {
    return stub.all();
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    return client.count(name(), kafkaPartition);
  }

}
