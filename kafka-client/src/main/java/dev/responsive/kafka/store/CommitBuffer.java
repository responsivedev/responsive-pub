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

import static dev.responsive.db.CassandraClient.UNSET_PERMIT;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraClient.OffsetRow;
import dev.responsive.model.Result;
import dev.responsive.utils.ExplodePartitioner;
import dev.responsive.utils.Iterators;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollector.Supplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitBuffer<K> implements RecordBatchingStateRestoreCallback {

  public static final int MAX_BATCH_SIZE = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(CommitBuffer.class);
  private static final int MAX_CONCURRENT_WRITES = 1024;

  private final NavigableMap<K, Result<K>> buffer;
  private final CassandraClient client;
  private final String tableName;
  private final int partition;
  private final Admin admin;
  private final Supplier recordCollector;
  private final TopicPartition changelog;
  private final BufferPlugin<K> plugin;
  private final ExplodePartitioner partitioner;

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Supplier recordCollector,
      final Admin admin,
      final BufferPlugin<K> plugin,
      final ExplodePartitioner partitioner) {
    this.client = client;
    this.tableName = tableName;
    this.recordCollector = recordCollector;
    this.changelog = changelog;
    this.partition = changelog.partition();
    this.admin = admin;
    this.plugin = plugin;

    this.buffer = new TreeMap<>(plugin);
    this.partitioner = partitioner;

    for (int i = 0; i < partitioner.getFactor(); i++) {
      client.initializeOffset(tableName, partitioner.base(partition) + i);
    }
  }

  public void put(final K key, final byte[] value) {
    buffer.put(key, Result.value(key, value));
  }

  public void tombstone(final K key) {
    buffer.put(key, Result.tombstone(key));
  }

  public Result<K> get(final K key) {
    final Result<K> result = buffer.get(key);
    if (result != null && plugin.retain(result.key)) {
      return result;
    }
    return null;
  }

  public KeyValueIterator<K, Result<K>> range(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.subMap(from, to).entrySet().iterator(),
            e -> plugin.retain(e.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> range(
      final K from,
      final K to,
      final Predicate<K> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.subMap(from, to).entrySet().iterator(),
            e -> plugin.retain(e.getKey()) && filter.test(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backRange(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.descendingMap().subMap(to, from).entrySet().iterator(),
            e -> plugin.retain(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> all(
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.entrySet().iterator(),
            e -> plugin.retain(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> all(
      final Predicate<K> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.entrySet().iterator(),
            kv -> plugin.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backAll(
      final Predicate<K> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.descendingMap().entrySet().iterator(),
            kv -> plugin.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  // Visible For Testing
  int size() {
    return buffer.size();
  }

  public void flush() {
    if (buffer.isEmpty()) {
      // no need to do anything if the buffer is empty
      LOG.info("Ignoring flush() to empty commit buffer for {}[{}]", tableName, partition);
      return;
    }

    // TODO: what happens if flush is called before there's any committed offsets?
    // TODO: this also won't work if the source-changelog optimization is used
    final RecordCollector collector = recordCollector.recordCollector();
    if (collector == null) {
      // this shouldn't happen? collector can be null if this instance
      // is either a standby replica or not transitioned yet to active,
      // but we don't run with standbys and flush should only happen
      // when active...
      throw new IllegalStateException(
          "Unexpected null record collector for " + tableName + "[" + partition + "]");
    }

    final Long offset = collector.offsets().get(changelog);
    if (offset == null) {
      throw new IllegalStateException(
          "Unexpected state: buffer is non-empty but no write has gone to changelog");
    }

    // TODO: support KIP-892 so the following is guaranteed not to happen
    // is it possible that a put request has been processed by the
    // buffer that is not covered by the offset extracted above? if
    // this is possible then we will be flushing data to Cassandra
    // that is not yet in the changelog
    final UUID txnid = UUID.randomUUID();
    final FlushResult flush = flush(offset, txnid);
    if (!flush.wasApplied) {
      final OffsetRow stored = client.getOffset(tableName, flush.partition);
      // we were fenced - the only conditional statement is the
      // offset update, so it's the only failure point
      throw new ProcessorStateException(
          "Failure to write batch to " + tableName + "[" + partition + ":" + flush.partition
              + "] with end " + "offset " + offset + " and stored offset " + stored.offset
              + ". If the stored offset is larger than the end offset it is likely that "
              + "this client was fenced by a more up to date consumer. txnId: " + txnid
              + " and stored txnId: " + stored.txind
      );
    }
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private FlushResult flush(final long offset, final UUID txnid) {
    LOG.info("Flushing {} records to remote {}[{}] (offset={}, transactionId={})",
        buffer.size(),
        tableName,
        partition,
        offset,
        txnid
    );

    final var entries = buffer.entrySet().iterator();
    final var builders = new HashMap<Integer, PartitionedBuilder>();

    for (int i = 0; i < partitioner.getFactor(); i++) {
      final int storePartition = partitioner.base(partition) + i;
      final var builder = new PartitionedBuilder(storePartition, txnid, offset);
      builders.put(storePartition, builder);
      builder.initBatch();
    }

    while (entries.hasNext()) {
      final Result<K> result = entries.next().getValue();
      final int storePartition = partitioner.repartition(partition, plugin.bytes(result.key));

      final var builder = builders.get(storePartition);

      if (result.isTombstone || plugin.retain(result.key)) {
        builder.add(result);
        if (builder.hitBatchLimit()) {
          builder.flush();
          builder.initBatch();
        }
      }
    }

    final var flushes = new ArrayList<CompletableFuture<FlushResult>>();
    for (PartitionedBuilder builder : builders.values()) {
      var stage = builder.flush();
      var result = stage.thenCompose(
          fr -> !fr.wasApplied
              ? CompletableFuture.completedStage(fr)
              : finalize(fr, txnid, offset));
      flushes.add(result.toCompletableFuture());

      if (flushes.size() % MAX_CONCURRENT_WRITES == 0) {
        for (CompletableFuture<FlushResult> flush : flushes) {
          try {
            flush.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    var iterator = flushes.iterator();
    while (iterator.hasNext()) {
      final CompletableFuture<FlushResult> flush = iterator.next();
      try {
        final FlushResult flushResult = flush.get();
        if (!flushResult.wasApplied) {
          return flushResult;
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new ProcessorStateException(
            String.format("Failed while flushing partition %s to remote", partition), e);
      } finally {
        iterator.forEachRemaining(fut -> fut.cancel(true));
      }
    }

    LOG.info("Completed flushing {} records to remote {}[{}] (offset={}, transactionId={})",
        buffer.size(),
        tableName,
        partition,
        offset,
        txnid
    );
    buffer.clear();

    try {
      admin.deleteRecords(Map.of(changelog, RecordsToDelete.beforeOffset(offset))).all().get();
      LOG.info("Truncated changelog topic {} before offset {}", changelog, offset);
    } catch (final ExecutionException e) {
      LOG.warn("Could not truncate changelog topic-partition {}.", changelog, e);
    } catch (final InterruptedException e) {
      throw new ProcessorStateException("Interrupted while truncating " + changelog, e);
    }

    return FlushResult.success();
  }

  private CompletionStage<FlushResult> finalize(
      final FlushResult fr,
      final UUID txnid,
      final long offset
  ) {
    return client.executeAsync(client.finalizeTxn(tableName, fr.partition, txnid, offset))
        .thenApply(ar -> FlushResult.fromResult(ar, fr.partition));
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    // it's OK to just check the first partition since a successful write will
    // have written the same offset to all the sub-partitions within the exploded
    // partition space
    final int basePartition = partitioner.base(partition);
    final long committedOffset = client.getOffset(tableName, partitioner.base(partition)).offset;

    long consumedOffset = -1L;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      if (record.offset() <= committedOffset) {
        continue;
      }

      consumedOffset = record.offset();
      if (record.value() == null) {
        tombstone(plugin.keyFromRecord(record));
      } else {
        put(plugin.keyFromRecord(record), record.value());
      }
    }

    if (consumedOffset >= 0) {
      final FlushResult result = flush(consumedOffset, null);
      if (!result.wasApplied) {
        // it is possible that this is a warmup replica that while restoring the
        // active replica is still writing and flushing to cassandra, in that
        // case just clear the buffer and try to restore the next batch
        buffer.clear();

        final OffsetRow stored = client.getOffset(tableName, partition);
        LOG.warn(
            "Restoration for {}[{}:{}] was fenced. There is likely an existing active "
                + "replica that is writing to Cassandra. Original Offset: {}, Batch Offset: "
                + "{}, Latest Offset: {}, Stored txnid: {}. This is not a problem but may "
                + "cause rebalancing to take longer.",
            tableName,
            partition,
            result,
            committedOffset,
            consumedOffset,
            stored.offset,
            stored.txind
        );
      }
    }
  }

  // use non-static class since this is only ever used internally
  // to this class
  private class PartitionedBuilder {

    final int partition;
    final UUID txnid;
    final long offset;

    BatchStatementBuilder builder;
    int size = 0;
    boolean firstBatch = true;

    CompletionStage<FlushResult> result;

    private PartitionedBuilder(final int partition, final UUID txnid, final long offset) {
      this.partition = partition;
      this.txnid = txnid;
      this.offset = offset;

      this.result = CompletableFuture.completedStage(new FlushResult(partition, true));
    }

    void initBatch() {
      if (size != 0) {
        return;
      }

      builder = new BatchStatementBuilder(BatchType.UNLOGGED);
      if (txnid != null) {
        builder.addStatement(client.acquirePermit(
            tableName,
            partition,
            firstBatch ? UNSET_PERMIT : txnid,
            txnid,
            offset
        ));
        firstBatch = false;
      } else {
        builder.addStatement(client.revokePermit(tableName, partition, offset));
      }

      builder.setIdempotence(true);
    }

    void add(final Result<K> result) {
      size++;
      if (result.isTombstone || plugin.retain(result.key)) {
        builder.addStatement(result.isTombstone
            ? plugin.deleteData(client, tableName, partition, result.key)
            : plugin.insertData(client, tableName, partition, result.key, result.value));
      }
    }

    boolean hitBatchLimit() {
      return size >= MAX_BATCH_SIZE;
    }

    CompletionStage<FlushResult> flush() {
      final var batch = builder.build();
      size = 0;

      result = result.thenCompose(fr -> !fr.wasApplied
          ? CompletableFuture.completedStage(fr)
          : execute(batch));
      return result;
    }

    CompletionStage<FlushResult> execute(final Statement<?> statement) {
      return client.executeAsync(statement)
          .thenApply(ar -> new FlushResult(partition, ar.wasApplied()));
    }

  }

  private static class FlushResult {

    final int partition;
    final boolean wasApplied;

    public static FlushResult fromResult(final AsyncResultSet result, final int partition) {
      return new FlushResult(partition, result.wasApplied());
    }

    public static FlushResult success() {
      return new FlushResult(-1, true);
    }

    private FlushResult(final int partition, final boolean wasApplied) {
      this.partition = partition;
      this.wasApplied = wasApplied;
    }
  }

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
}