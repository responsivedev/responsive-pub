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
import dev.responsive.kafka.internal.stores.WindowStoreStub;
import dev.responsive.kafka.internal.utils.WindowedKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDWindowedTable extends TTDTable<WindowedKey>
    implements RemoteWindowedTable<BoundStatement>  {

  private final String name;
  private final WindowStoreStub stub;

  public static TTDWindowedTable create(
      final CassandraTableSpec spec,
      final CassandraClient client
  ) {
    return new TTDWindowedTable(spec, (TTDCassandraClient) client);
  }

  public TTDWindowedTable(final CassandraTableSpec spec, final TTDCassandraClient client) {
    super(client);
    name = spec.tableName();
    stub = new WindowStoreStub();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public BoundStatement insert(
      final int kafkaPartition,
      final WindowedKey key,
      final byte[] value,
      final long epochMillis
  ) {
    stub.put(key, value);
    return null;
  }

  @Override
  public BoundStatement delete(
      final int kafkaPartition,
      final WindowedKey key
  ) {
    stub.delete(key);
    return null;
  }

  @Override
  public byte[] fetch(
      int kafkaPartition,
      Bytes key,
      long windowStart
  ) {
    return stub.fetch(key, windowStart);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return stub.backFetchAll(timeFrom, timeTo);
  }

  @Override
  public long count() {
    return 0;
  }
}
