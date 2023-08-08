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

package dev.responsive.db;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.db.partitioning.SubPartitioner;
import javax.annotation.CheckReturnValue;

/**
 * A {@code RemoteSchema} defines the access pattern to the remote store for a given table schema as
 * well as the metadata schema.
 */
public interface RemoteSchema<K> {

  /** Creates a table with the supplied {@code tableName} with the desired schema. */
  void create(String tableName);

  /** Prepares statements with this schema for the table with {@code tableName}. */
  void prepare(final String tableName);

  /** Generates a statement that can insert data into the table specified. */
  @CheckReturnValue
  BoundStatement insert(
      final String table, final int partitionKey, final K key, final byte[] value);

  /** Generates a statement that can delete data into the table specified. */
  @CheckReturnValue
  BoundStatement delete(final String table, final int partitionKey, final K key);

  /**
   * Initializes the table by setting the metadata fields to their initialized values.
   *
   * @return a {@link FencingToken} that gives the callee access to run statements on {@code table}
   */
  FencingToken init(final String table, final SubPartitioner partitioner, final int kafkaPartition);

  /**
   * Returns the metadata for the given table/partition, note that implementations may return
   * partially filled metadata if the schema for that table does not contain such metadata.
   */
  // TODO: we should parameterized RemoteSchema on the metadata type
  MetadataRow metadata(final String table, final int partition);

  /**
   * Generates a statement that can be used to set the offset in the metadata row of {@code table}.
   */
  @CheckReturnValue
  BoundStatement setOffset(
      final String table, final FencingToken token, final int partition, final long offset);

  CassandraClient getClient();
}
