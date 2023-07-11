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

package dev.responsive.kafka.store;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.db.CassandraClient;
import java.util.Comparator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

/**
 * Used to ensure that {@link CommitBuffer} can work both with
 * windowed and key-value data.
 */
interface BufferPlugin<K> extends Comparator<K> {

  K keyFromRecord(final ConsumerRecord<byte[], byte[]> record);

  BoundStatement insertData(
      final CassandraClient client,
      final String tableName,
      final int partition,
      final K key,
      final byte[] value
  );

  BoundStatement deleteData(
      final CassandraClient client,
      final String tableName,
      final int partition,
      final K key
  );

  default boolean retain(final K key) {
    return true;
  }

  Bytes bytes(final K key);
}
