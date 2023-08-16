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
import dev.responsive.db.RemoteKeyValueSchema;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class TTDKeyValueSchema extends TTDSchema<Bytes> implements RemoteKeyValueSchema {

  private final Map<String, KVStoreStub> tableNameToStore = new HashMap<>();

  public TTDKeyValueSchema(final CassandraClientStub client) {
    super(client);
  }

  @Override
  public SimpleStatement create(final String tableName, final Optional<Duration> ttl) {
    tableNameToStore.put(tableName, new KVStoreStub(ttl.orElse(null), time));
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
      final Bytes key,
      final byte[] value
  ) {
    tableNameToStore.get(tableName).put(key, value);
    return null;
  }

  @Override
  public BoundStatement delete(final String tableName, final int partitionKey, final Bytes key) {
    tableNameToStore.get(tableName).delete(key);
    return null;
  }

  @Override
  public byte[] get(final String tableName, final int partition, final Bytes key) {
    return tableNameToStore.get(tableName).get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final String tableName,
      final int partition,
      Bytes from,
      final Bytes to
  ) {
    return tableNameToStore.get(tableName).range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final String tableName, final int partition) {
    return tableNameToStore.get(tableName).all();
  }
}
