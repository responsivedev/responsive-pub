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

package dev.responsive.kafka.api;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import dev.responsive.db.RemoteWindowedSchema;
import dev.responsive.model.Stamped;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDWindowedSchema extends TTDSchema<Stamped<Bytes>> implements RemoteWindowedSchema {

  private final Map<String, WindowStoreStub> tableNameToStore = new HashMap<>();

  public TTDWindowedSchema(final CassandraClientStub client) {
    super(client);
  }

  @Override
  public SimpleStatement create(final String tableName, final Optional<Duration> ttl) {
    tableNameToStore.put(tableName, new WindowStoreStub());
    return null;
  }

  @Override
  public long count(final String tableName) {
    return tableNameToStore.containsKey(tableName) ? tableNameToStore.get(tableName).count() : 0L;
  }

  @Override
  public BoundStatement insert(
      final String tableName,
      final int partitionKey,
      final Stamped<Bytes> key,
      final byte[] value
  ) {
    tableNameToStore.get(tableName).put(key, value);
    return null;
  }

  @Override
  public BoundStatement delete(
      final String tableName,
      final int partitionKey,
      final Stamped<Bytes> key
  ) {
    tableNameToStore.get(tableName).delete(key);
    return null;
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetch(
      final String tableName,
      final int partition,
      final Bytes key,
      long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).fetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetch(
      final String tableName,
      final int partition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).backFetch(key, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).fetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchRange(
      final String tableName,
      final int partition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).backFetchRange(fromKey, toKey, timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> fetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).fetchAll(timeFrom, timeTo);
  }

  @Override
  public KeyValueIterator<Stamped<Bytes>, byte[]> backFetchAll(
      final String tableName,
      final int partition,
      final long timeFrom,
      final long timeTo
  ) {
    return tableNameToStore.get(tableName).backFetchAll(timeFrom, timeTo);
  }
}
