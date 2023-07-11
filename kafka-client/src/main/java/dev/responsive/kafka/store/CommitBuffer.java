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

import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraClient.OffsetRow;
import dev.responsive.model.Result;
import dev.responsive.utils.Iterators;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitBuffer<K> implements RecordBatchingStateRestoreCallback {

  public static final int MAX_BATCH_SIZE = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(CommitBuffer.class);

  private final NavigableMap<K, Result<K>> buffer;
  private final CassandraClient client;
  private final String tableName;
  private final int partition;
  private final Admin admin;
  private final TopicPartition changelog;
  private final BufferPlugin<K> plugin;
  private final boolean truncateChangelog;
  private final int flushThreshold;
  private final int maxBatchSize;

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Admin admin,
      final BufferPlugin<K> plugin,
      final boolean truncateChangelog,
      final int flushThreshold
  ) {
    this(
        client,
        tableName,
        changelog,
        admin,
        plugin,
        truncateChangelog,
        flushThreshold,
        MAX_BATCH_SIZE
    );
  }

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Admin admin,
      final BufferPlugin<K> plugin,
      final boolean truncateChangelog,
      final int flushThreshold,
      final int maxBatchSize
  ) {
    this.client = client;
    this.tableName = tableName;
    this.changelog = changelog;
    this.partition = changelog.partition();
    this.admin = admin;
    this.plugin = plugin;

    this.buffer = new TreeMap<>(plugin);
    this.truncateChangelog = truncateChangelog;
    this.flushThreshold = flushThreshold;
    this.maxBatchSize = maxBatchSize;
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

  long offset() {
    return client.getOffset(tableName, partition).offset;
  }

  // Visible For Testing
  int size() {
    return buffer.size();
  }

  public void flush(long offset) {
    if (buffer.size() < flushThreshold) {
      LOG.debug("Ignoring flush() - commit buffer {} smaller than flush threshold {}",
          buffer.size(),
          flushThreshold
      );
      return;
    }

    if (buffer.isEmpty()) {
      // no need to do anything if the buffer is empty
      LOG.info("Ignoring flush() to empty commit buffer for {}[{}]", tableName, partition);
      return;
    }

    // TODO: support KIP-892 so the following is guaranteed not to happen
    // is it possible that a put request has been processed by the
    // buffer that is not covered by the offset extracted above? if
    // this is possible then we will be flushing data to Cassandra
    // that is not yet in the changelog
    final UUID txnid = UUID.randomUUID();
    final var result = flush(offset, txnid, maxBatchSize);
    if (!result.wasApplied()) {
      final OffsetRow stored = client.getOffset(tableName, partition);
      // we were fenced - the only conditional statement is the
      // offset update, so it's the only failure point
      throw new ProcessorStateException(
          "Failure to write batch to " + tableName + "[" + partition + "] with end offset " + offset
              + " and stored offset " + stored.offset + ". If the stored offset is "
              + "larger than the end offset it is likely that this client was fenced by a more "
              + "up to date consumer. txnId: " + txnid + " and stored txnId: " + stored.txind
      );
    }
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private AtomicWriteResult flush(final long offset, final UUID txnid, final int batchSize) {
    LOG.info("Flushing {} records to remote {}[{}] (offset={}, transactionId={})",
        buffer.size(),
        tableName,
        partition,
        offset,
        txnid
    );

    final var writer = new AtomicWriter<K>(
        client,
        tableName,
        partition,
        plugin,
        offset,
        txnid,
        batchSize
    );

    writer.addAll(buffer.values());

    AtomicWriteResult result;
    try {
      result = writer.flush().toCompletableFuture().get();
      if (result.wasApplied()) {
        result = writer.finalizeTxn().toCompletableFuture().get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new ProcessorStateException(
          "Could not flush to remote storage for partition " + partition, e);
    }

    LOG.info("Completed flushing {} records to remote {}[{}] (offset={}, transactionId={})",
        buffer.size(),
        tableName,
        partition,
        offset,
        txnid
    );
    buffer.clear();

    if (truncateChangelog) {
      try {
        admin.deleteRecords(Map.of(changelog, RecordsToDelete.beforeOffset(offset))).all().get();
        LOG.info("Truncated changelog topic {} before offset {}", changelog, offset);
      } catch (final ExecutionException e) {
        LOG.warn("Could not truncate changelog topic-partition {}.", changelog, e);
      } catch (final InterruptedException e) {
        throw new ProcessorStateException("Interrupted while truncating " + changelog, e);
      }
    }

    return result;
  }

  @Override
  public void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    final List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(maxBatchSize);
    for (final ConsumerRecord<byte[], byte[]> r : records) {
      batch.add(r);
      if (batch.size() >= maxBatchSize) {
        restoreCassandraBatch(batch);
        batch.clear();
      }
    }
    if (batch.size() > 0) {
      restoreCassandraBatch(batch);
    }
  }

  private void restoreCassandraBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
    final long committedOffset = client.getOffset(tableName, partition).offset;

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
      final var flush = flush(consumedOffset, null, records.size());
      if (!flush.wasApplied()) {
        // it is possible that this is a warmup replica that while restoring the
        // active replica is still writing and flushing to cassandra, in that
        // case just clear the buffer and try to restore the next batch
        buffer.clear();

        final OffsetRow stored = client.getOffset(tableName, partition);
        LOG.warn(
            "Restoration for {}[{}] was fenced. There is likely an existing active "
                + "replica that is writing to the remote store. Original Offset: {}, Batch Offset: "
                + "{}, Latest Offset: {}, Stored txnid: {}. This is not a problem but may "
                + "cause rebalancing to take longer.",
            tableName,
            partition,
            committedOffset,
            consumedOffset,
            stored.offset,
            stored.txind
        );
      }
    }
  }

}
