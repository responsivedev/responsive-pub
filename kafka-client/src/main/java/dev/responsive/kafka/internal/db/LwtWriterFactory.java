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

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;

public class LwtWriterFactory<K, P> extends WriterFactory<K, P> {

  private final RemoteTable<K, BoundStatement> table;
  private final TableMetadata<P> tableMetadata;
  private final CassandraClient client;
  private final TablePartitioner<K, P> partitioner;
  private final int kafkaPartition;
  private final long epoch;

  public LwtWriterFactory(
      final RemoteTable<K, BoundStatement> table,
      final TableMetadata<P> tableMetadata,
      final CassandraClient client,
      final TablePartitioner<K, P> partitioner,
      final int kafkaPartition,
      final long epoch
  ) {
    super(
        String.format("LwtWriterFactory{epoch=%d} ", epoch)
    );
    this.table = table;
    this.tableMetadata = tableMetadata;
    this.client = client;
    this.partitioner = partitioner;
    this.kafkaPartition = kafkaPartition;
    this.epoch = epoch;
  }

  @Override
  public RemoteWriter<K, P> createWriter(final P tablePartition) {
    return new LwtWriter<>(
        client,
        () -> tableMetadata.ensureEpoch(tablePartition, epoch),
        table,
        kafkaPartition,
        tablePartition
    );
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  protected P tablePartitionForKey(final K key) {
    return partitioner.tablePartition(kafkaPartition, key);
  }

  @Override
  public RemoteWriteResult<P> setOffset(final long offset) {
    final P tablePartition = partitioner.metadataTablePartition(kafkaPartition);

    final BatchStatementBuilder builder = new BatchStatementBuilder(BatchType.UNLOGGED);
    builder.addStatement(fencingStatement(tablePartition));
    builder.addStatement(table.setOffset(kafkaPartition, offset));

    // TODO(sophie): clean up this hack, perhaps by combining the offset and stream-time into
    //  a single metadata row update
    if (table instanceof CassandraWindowedTable) {
      builder.addStatement(((CassandraWindowedTable) table).setStreamTime(kafkaPartition, epoch));
    }

    final var result = client.execute(builder.build());
    return result.wasApplied()
        ? RemoteWriteResult.success(tablePartition)
        : RemoteWriteResult.failure(tablePartition);
  }

  @Override
  protected long offset() {
    return table.fetchOffset(kafkaPartition);
  }

  @Override
  public RemoteWriteResult<P> commitPendingFlush(
      final PendingFlush pendingFlush,
      final long consumedOffset
  ) {
    tableMetadata.preCommit(kafkaPartition, epoch);

    final var flushResult = super.commitPendingFlush(pendingFlush, consumedOffset);
    tableMetadata.postCommit(kafkaPartition, epoch);

    return flushResult;
  }

  @Override
  public String failedFlushError(
      final RemoteWriteResult<P> result,
      final long consumedOffset
  ) {
    final String baseErrorMsg = super.failedFlushError(result, consumedOffset);

    final long storedEpoch = tableMetadata.fetchEpoch(result.tablePartition());

    // this most likely is a fencing error, so make sure to add on all the information
    // that is relevant to fencing in the error message
    return baseErrorMsg
        + String.format(", Local Epoch: %s, Persisted Epoch: %d", epoch, storedEpoch);
  }

  private BoundStatement fencingStatement(final P tablePartition) {
    return tableMetadata.ensureEpoch(tablePartition, epoch);
  }
}
