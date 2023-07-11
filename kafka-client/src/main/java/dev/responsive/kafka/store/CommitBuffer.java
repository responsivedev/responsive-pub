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
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.SubPartitioner;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
  private final FlushTriggers flushTriggers;
  private final int maxBatchSize;
  private final SubPartitioner subPartitioner;
  private final Supplier<Instant> clock;

  private Instant lastFlush;

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Admin admin,
      final BufferPlugin<K> plugin,
      final boolean truncateChangelog,
      final FlushTriggers flushTriggers,
      final SubPartitioner subPartitioner
  ) {
    this(
        client,
        tableName,
        changelog,
        admin,
        plugin,
        truncateChangelog,
        flushTriggers,
        MAX_BATCH_SIZE,
        subPartitioner,
        Instant::now
    );
  }

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Admin admin,
      final BufferPlugin<K> plugin,
      final boolean truncateChangelog,
      final FlushTriggers flushTriggers,
      final int maxBatchSize,
      final SubPartitioner subPartitioner,
      final Supplier<Instant> clock
  ) {
    this.client = client;
    this.tableName = tableName;
    this.changelog = changelog;
    this.partition = changelog.partition();
    this.admin = admin;
    this.plugin = plugin;

    this.buffer = new TreeMap<>(plugin);
    this.truncateChangelog = truncateChangelog;
    this.flushTriggers = flushTriggers;
    this.maxBatchSize = maxBatchSize;
    this.subPartitioner = subPartitioner;
    this.clock = clock;
    lastFlush = clock.get();
  }

  public void init() {
    subPartitioner.all(partition).forEach(p -> client.initializeOffset(tableName, p));

    // when we initialize a new state store, we should revoke all ongoing
    // transactions to the remote store - otherwise it is possible that
    // there's a hanging transaction that will fence this writer if there
    // is no data to restore
    final long offset = offset();
    if (offset >= 0) {
      subPartitioner
          .all(partition)
          .forEach(p -> client.execute(client.revokePermit(tableName, p, offset)));
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

  long offset() {
    return subPartitioner.all(partition)
        .mapToLong(p -> client.getOffset(tableName, p).offset)
        .min()
        .orElseThrow(() -> new IllegalStateException(
            "Expected an offset to exist in remote for kafka partition " + partition
                + ". If you see this error it means that init() was not called on "
                + "the CommitBuffer.")
        );
  }

  // Visible For Testing
  int size() {
    return buffer.size();
  }

  private long totalBytesBuffered() {
    return buffer.values().stream()
        .mapToLong(e -> plugin.bytes(e.key).get().length + (e.isTombstone ? 0 : e.value.length))
        .sum();
  }

  private boolean shouldFlush() {
    boolean recordsTrigger = false;
    boolean bytesTrigger = false;
    boolean timeTrigger = false;
    if (buffer.size() >= flushTriggers.getRecords()) {
      LOG.info("will flush due to records buffered {} over trigger {}",
          buffer.size(),
          flushTriggers.getRecords()
      );
      recordsTrigger = true;
    } else {
      LOG.debug("records buffered {} not over trigger {}",
          buffer.size(),
          flushTriggers.getRecords()
      );
    }
    // This is a little inefficient (as opposed to maintaining the size as we go along).
    // On the other hand, it's much less error-prone.
    final long totalBytesBuffered = totalBytesBuffered();
    if (totalBytesBuffered >= flushTriggers.getBytes()) {
      LOG.info("will flush due to bytes buffered {} over bytes trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
      bytesTrigger = true;
    } else {
      LOG.debug("bytes buffered {} not over trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
    }
    final var now = clock.get();
    if (lastFlush.plus(flushTriggers.getInterval()).isBefore(now)) {
      LOG.info("will flush due to time since last flush {} over interval trigger {}",
          Duration.between(lastFlush, now),
          now
      );
      timeTrigger = true;
    } else {
      LOG.debug("time since last flush {} not over trigger {}",
          Duration.between(lastFlush, now),
          now
      );
    }
    return recordsTrigger || bytesTrigger || timeTrigger;
  }

  public void flush(long offset) {
    if (!shouldFlush()) {
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
    final var writeResult = flush(offset, txnid, maxBatchSize);
    if (!writeResult.wasApplied()) {
      final OffsetRow stored = client.getOffset(tableName, partition);
      // we were fenced - the only conditional statement is the
      // offset update, so it's the only failure point
      throw new ProcessorStateException(
          String.format(
              "Failure to write batch to %s[%d:%d] with end offset %d and stored offset %d. "
                  + "If the stored offset is larger than the end offset it is likely that "
                  + "this client was fenced by a more up to date consumer. txnId: %s and "
                  + "stored txnId: %s",
              tableName,
              partition,
              writeResult.getPartition(),
              offset,
              stored.offset,
              txnid,
              stored.txind
          )
      );
    }
    lastFlush = clock.get();
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

    final var writers = new HashMap<Integer, AtomicWriter<K>>();
    subPartitioner.all(partition).forEach(p -> writers.put(p, new AtomicWriter<>(
        client,
        tableName,
        p,
        plugin,
        offset,
        txnid,
        batchSize
    )));

    buffer.values().forEach(val ->
        writers
            .get(subPartitioner.partition(partition, plugin.bytes(val.key)))
            .add(val)
    );

    var writeResult = drain(writers.values(), AtomicWriter::flush);
    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    writeResult = drain(writers.values(), AtomicWriter::finalizeTxn);
    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    LOG.info("Completed flushing {} records to remote {}[{}] (offset={}, transactionId={})",
        buffer.size(),
        tableName,
        partition,
        offset,
        txnid
    );
    buffer.clear();

    maybeTruncateChangelog(offset);
    return AtomicWriteResult.success(partition);
  }

  private AtomicWriteResult drain(
      final Collection<AtomicWriter<K>> writers,
      final Function<AtomicWriter<K>, CompletionStage<AtomicWriteResult>> action
  ) {
    return drain(
        writers,
        action,
        AtomicWriter::partition,
        partition,
        ResponsiveConfig.MAX_CONCURRENT_REMOTE_WRITES_DEFAULT
    );
  }

  // Visible for Testing
  static <I> AtomicWriteResult drain(
      final Collection<I> writers,
      final Function<I, CompletionStage<AtomicWriteResult>> action,
      final Function<I, Integer> keyFunction,
      final int partition,
      final int maxConcurrentWrites
  ) {
    final var results = new HashMap<Integer, CompletableFuture<AtomicWriteResult>>();
    try {
      final AtomicReference<AtomicWriteResult> failed = new AtomicReference<>(null);

      for (final I writer : writers) {
        if (results.size() >= maxConcurrentWrites) {
          CompletableFuture.anyOf(results.values().toArray(CompletableFuture[]::new)).get();
          results.values().removeIf(CompletableFuture::isDone);
        }

        if (failed.get() != null) {
          return failed.get();
        }

        results.put(
            keyFunction.apply(writer),
            action.apply(writer).thenApply(r -> maybeSetFailure(failed, r)).toCompletableFuture());
      }

      // drain any remaining
      for (final var future : results.values()) {
        final var result = future.get();
        if (!result.wasApplied()) {
          return result;
        }
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new ProcessorStateException(
          "Failed while flushing partition " + partition + " to remote", e);
    } finally {
      results.values().forEach(fut -> fut.cancel(true));
    }

    return AtomicWriteResult.success(partition);
  }

  private static AtomicWriteResult maybeSetFailure(
      final AtomicReference<AtomicWriteResult> failed,
      final AtomicWriteResult awr
  ) {
    if (!awr.wasApplied()) {
      failed.set(awr);
    }
    return awr;
  }

  private void maybeTruncateChangelog(final long offset) {
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
    final long committedOffset = offset();

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
      final var writeResult = flush(consumedOffset, null, records.size());
      if (!writeResult.wasApplied()) {
        // it is possible that this is a warmup replica that while restoring the
        // active replica is still writing and flushing to cassandra, in that
        // case just clear the buffer and try to restore the next batch
        buffer.clear();

        final OffsetRow stored = client.getOffset(tableName, writeResult.getPartition());
        LOG.warn(
            "Restoration for {}[{}:{}] was fenced. There is likely an existing active "
                + "replica that is writing to the remote store. Original Offset: {}, Batch Offset: "
                + "{}, Latest Offset: {}, Stored txnid: {}. This is not a problem but may "
                + "cause rebalancing to take longer.",
            tableName,
            partition,
            writeResult.getPartition(),
            committedOffset,
            consumedOffset,
            stored.offset,
            stored.txind
        );
      }
    }
  }

}
