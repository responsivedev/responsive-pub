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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static dev.responsive.db.ColumnNames.EPOCH;
import static dev.responsive.db.ColumnNames.OFFSET;
import static dev.responsive.db.ColumnNames.PARTITION_KEY;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckReturnValue;

public class MetadataStatements {

  private final CassandraClient client;
  private final ConcurrentHashMap<String, PreparedStatement> getMetadata;
  private final ConcurrentHashMap<String, PreparedStatement> reserveEpoch;
  private final ConcurrentHashMap<String, PreparedStatement> ensureEpoch;
  private final ConcurrentHashMap<String, PreparedStatement> setOffset;
  private final MetadataKeys metadataKeys;

  public MetadataStatements(
      final CassandraClient client,
      final MetadataKeys metadataKeys
  ) {
    this.client = client;
    getMetadata = new ConcurrentHashMap<>();
    reserveEpoch = new ConcurrentHashMap<>();
    ensureEpoch = new ConcurrentHashMap<>();
    setOffset = new ConcurrentHashMap<>();
    this.metadataKeys = metadataKeys;
  }

  public void prepare(final String tableName) {
    getMetadata.computeIfAbsent(tableName, k -> client.prepare(
        metadataKeys.addMetaColumnsToWhere(QueryBuilder
            .selectFrom(tableName)
            .column(EPOCH.column())
            .column(OFFSET.column())
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind()))))
            .build()
    ));

    setOffset.computeIfAbsent(tableName, k -> client.prepare(
        metadataKeys.addMetaColumnsToWhere(QueryBuilder
            .update(tableName)
            .setColumn(OFFSET.column(), bindMarker(OFFSET.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind()))))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));

    reserveEpoch.computeIfAbsent(tableName, k -> client.prepare(
        metadataKeys.addMetaColumnsToWhere(QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind()))))
            .ifColumn(EPOCH.column()).isLessThan(bindMarker(EPOCH.bind()))
            .build()
    ));

    ensureEpoch.computeIfAbsent(tableName, k -> client.prepare(
        metadataKeys.addMetaColumnsToWhere(QueryBuilder
            .update(tableName)
            .setColumn(EPOCH.column(), bindMarker(EPOCH.bind()))
            .where(PARTITION_KEY.relation().isEqualTo(bindMarker(PARTITION_KEY.bind()))))
            .ifColumn(EPOCH.column()).isEqualTo(bindMarker(EPOCH.bind()))
            .build()
    ));
  }

  /**
   * Initializes the metadata entry for {@code table} by adding a
   * row with key {@code _metadata} and sets special columns
   * {@link ColumnNames#OFFSET} and {@link ColumnNames#EPOCH}.
   *
   * <p>The {@code partitionKey} is the unit of atomicity for the
   * offset compare-and-check operation. Two operations that run
   * concurrently for different {@code partitionKey} values will
   * not be fenced.
   *
   * <p>Note that this method is idempotent as it uses Cassandra's
   * {@code IF NOT EXISTS} functionality.
   *
   * @param table         the table that is initialized
   * @param partitionKey  the partition to initialize
   */
  public void initializeMetadata(
      final String table,
      final int partitionKey
  ) {
    // TODO: what happens if the user has data with the key "_offset"?
    // we should consider using a special serialization format for keys
    // (e.g. adding a magic byte of 0x00 to the offset and 0x01 to all
    // th data keys) so that it's impossible for a collision to happen
    final RegularInsert insert = metadataKeys.addKeyColumnsToInitMetadata(
        QueryBuilder.insertInto(table)
        .value(PARTITION_KEY.column(), PARTITION_KEY.literal(partitionKey)))
        .value(OFFSET.column(), OFFSET.literal(-1L))
        .value(EPOCH.column(), QueryBuilder.literal(0L));

    client.execute(insert.ifNotExists().build());
  }

  @CheckReturnValue
  public BoundStatement reserveEpoch(
      final String table,
      final int partitionKey,
      final long epoch
  ) {
    return reserveEpoch.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(EPOCH.bind(), epoch);
  }

  @CheckReturnValue
  public BoundStatement ensureEpoch(
      final String table,
      final int partitionKey,
      final long epoch
  ) {
    return ensureEpoch.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(EPOCH.bind(), epoch);
  }

  @CheckReturnValue
  public BoundStatement setOffset(
      final String table,
      final int partitionKey,
      final long offset,
      final long epoch
  ) {
    return setOffset.get(table)
        .bind()
        .setInt(PARTITION_KEY.bind(), partitionKey)
        .setLong(OFFSET.bind(), offset)
        .setLong(EPOCH.bind(), epoch);
  }

  /**
   * Get the metadata row for the table/partition pairing which includes:
   * <ol>
   *   <li>The current (last committed) offset</li>
   *   <li>The current writer epoch</li>
   * </ol>
   *
   * @param tableName the table
   * @param partition the table partition
   *
   * @return the metadata row
   */
  public MetadataRow metadata(final String tableName, final int partition) {
    final BoundStatement bound = getMetadata.get(tableName)
        .bind()
        .setInt(PARTITION_KEY.bind(), partition);
    final List<Row> result = client.execute(bound).all();

    if (result.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected exactly one offset row for %s[%s] but got %d",
          tableName, partition, result.size()));
    } else {
      return new MetadataRow(
          result.get(0).getLong(OFFSET.column()),
          result.get(0).getLong(EPOCH.column())
      );
    }
  }

  public long count(final String tableName, final int partition) {
    return client.count(tableName, partition);
  }

  public interface MetadataKeys {
    <T extends OngoingWhereClause<T>> T addMetaColumnsToWhere(final T builder);

    RegularInsert addKeyColumnsToInitMetadata(final RegularInsert insert);
  }

}
