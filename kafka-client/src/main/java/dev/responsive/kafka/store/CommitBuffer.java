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

import static dev.responsive.kafka.config.ResponsiveConfig.MAX_CONCURRENT_REMOTE_WRITES_CONFIG;

import dev.responsive.db.CassandraClient;
import dev.responsive.db.CassandraClient.MetadataRow;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.kafka.clients.SharedClients;
import dev.responsive.kafka.config.ResponsiveConfig;
import dev.responsive.model.Result;
import dev.responsive.utils.Iterators;
import dev.responsive.utils.StoreUtil;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

class CommitBuffer<K> implements RecordBatchingStateRestoreCallback {

  public static final int MAX_BATCH_SIZE = 1000;
  private final Logger log;

  private final SizeTrackingBuffer<K> buffer;
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
  private final int maxConcurrentWrites;

  private Instant lastFlush;
  private long epoch;

  static <K> CommitBuffer<K> from(
      final SharedClients clients,
      final TableName tableName,
      final TopicPartition changelog,
      final BufferPlugin<K> plugin,
      final ResponsiveConfig config
  ) {
    final var admin = clients.admin;
    final var cassandraClient = clients.cassandraClient;
    final var partitioner = config.getSubPartitioner(
        cassandraClient, admin, tableName, changelog.topic());
    final boolean truncate = StoreUtil.shouldTruncateChangelog(
        changelog.topic(), admin, config.originals());

    return new CommitBuffer<>(
        cassandraClient,
        tableName.cassandraName(),
        changelog,
        admin,
        plugin,
        truncate,
        FlushTriggers.fromConfig(config),
        partitioner,
        config.getInt(MAX_CONCURRENT_REMOTE_WRITES_CONFIG)
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
      final SubPartitioner subPartitioner,
      final int maxConcurrentWrites
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
        Instant::now,
        maxConcurrentWrites
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
      final Supplier<Instant> clock,
      final int maxConcurrentWrites
  ) {
    this.client = client;
    this.tableName = tableName;
    this.changelog = changelog;
    this.partition = changelog.partition();
    this.admin = admin;
    this.plugin = plugin;

    this.buffer = new SizeTrackingBuffer<>(plugin);
    this.truncateChangelog = truncateChangelog;
    this.flushTriggers = flushTriggers;
    this.maxBatchSize = maxBatchSize;
    this.subPartitioner = subPartitioner;
    this.clock = clock;
    this.maxConcurrentWrites = maxConcurrentWrites;

    final String logPrefix = String.format("commit-buffer-%s[%d] ", tableName, partition);
    final LogContext logContext = new LogContext(logPrefix);
    log = logContext.logger(CommitBuffer.class);
  }

  public void init() {
    subPartitioner.all(partition).forEach(p -> client.initializeMetadata(tableName, p));
    lastFlush = clock.get();

    // attempt to reserve an epoch - all epoch reservations will be done
    // under the first sub-partition and then "broadcast" to the other
    // partitions
    final int basePartition = subPartitioner.first(partition);
    this.epoch = client.metadata(tableName, basePartition).epoch + 1;

    for (final int subPartition : subPartitioner.all(partition).toArray()) {
      final var setEpoch = client.execute(client.reserveEpoch(tableName, subPartition, epoch));
      if (!setEpoch.wasApplied()) {
        final long otherEpoch = client.metadata(tableName, subPartition).epoch;
        final var msg = String.format(
            "Could not initialize commit buffer - attempted to claim epoch %d, but was fenced "
                + "by a writer that claimed epoch %d on partition %d",
            epoch,
            otherEpoch,
            subPartition
        );
        final var e = new TaskMigratedException(msg);
        log.warn(msg, e);
        throw e;
      }
    }
    log.info("Initialized store with epoch {} for all subpartitions {} -> {}",
        epoch, basePartition, basePartition + subPartitioner.getFactor() - 1);
  }

  public void put(final K key, final byte[] value) {
    buffer.put(key, Result.value(key, value));
  }

  public void tombstone(final K key) {
    buffer.put(key, Result.tombstone(key));
  }

  public Result<K> get(final K key) {
    final Result<K> result = buffer.getReader().get(key);
    if (result != null && plugin.retain(result.key)) {
      return result;
    }
    return null;
  }

  public KeyValueIterator<K, Result<K>> range(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().subMap(from, to).entrySet().iterator(),
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
            buffer.getReader().subMap(from, to).entrySet().iterator(),
            e -> plugin.retain(e.getKey()) && filter.test(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backRange(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().descendingMap().subMap(to, from).entrySet().iterator(),
            e -> plugin.retain(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> all(
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().entrySet().iterator(),
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
            buffer.getReader().entrySet().iterator(),
            kv -> plugin.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backAll(
      final Predicate<K> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().descendingMap().entrySet().iterator(),
            kv -> plugin.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  long offset() {
    return subPartitioner.all(partition)
        .mapToLong(p -> client.metadata(tableName, p).offset)
        .min()
        .orElseThrow(() -> new IllegalStateException(
            "Expected an offset to exist in remote for kafka partition " + partition
                + ". If you see this error it means that init() was not called on "
                + "the CommitBuffer.")
        );
  }

  // Visible For Testing
  int size() {
    return buffer.getReader().size();
  }

  private long totalBytesBuffered() {
    return buffer.getBytes();
  }

  private boolean shouldFlush() {
    boolean recordsTrigger = false;
    boolean bytesTrigger = false;
    boolean timeTrigger = false;
    if (buffer.getReader().size() >= flushTriggers.getRecords()) {
      log.info("will flush due to records buffered {} over trigger {}",
          buffer.getReader().size(),
          flushTriggers.getRecords()
      );
      recordsTrigger = true;
    } else {
      log.debug("records buffered {} not over trigger {}",
          buffer.getReader().size(),
          flushTriggers.getRecords()
      );
    }
    final long totalBytesBuffered = totalBytesBuffered();
    if (totalBytesBuffered >= flushTriggers.getBytes()) {
      log.info("will flush due to bytes buffered {} over bytes trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
      bytesTrigger = true;
    } else {
      log.debug("bytes buffered {} not over trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
    }
    final var now = clock.get();
    if (lastFlush.plus(flushTriggers.getInterval()).isBefore(now)) {
      log.info("will flush due to time since last flush {} over interval trigger {}",
          Duration.between(lastFlush, now),
          now
      );
      timeTrigger = true;
    } else {
      log.debug("time since last flush {} not over trigger {}",
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

    if (buffer.getReader().isEmpty()) {
      // no need to do anything if the buffer is empty
      log.info("Ignoring flush() to empty commit buffer");
      return;
    }

    final var writeResult = flush(offset, maxBatchSize);
    if (!writeResult.wasApplied()) {
      throwFencedException(writeResult, offset);
    }
    lastFlush = clock.get();
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private AtomicWriteResult flush(final long offset, final int batchSize) {
    log.info("Flushing {} records to remote (offset={}, epoch={})",
        buffer.getReader().size(),
        offset,
        epoch
    );

    final var writers = new HashMap<Integer, AtomicWriter<K>>();
    subPartitioner.all(partition).forEach(p -> writers.put(p, new AtomicWriter<>(
        client,
        tableName,
        p,
        plugin,
        epoch,
        batchSize
    )));

    buffer.getReader().values().forEach(val ->
        writers
            .get(subPartitioner.partition(partition, plugin.bytes(val.key)))
            .add(val)
    );

    var writeResult = drain(writers.values(), AtomicWriter::flush);
    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    // this offset is only used for recovery, so it can (and should) be done only
    // when all the flushes above have completed
    writeResult = drain(writers.values(), writer -> writer.setOffset(offset));
    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    log.info("Completed flushing {} records to remote (offset={}, epoch={})",
        buffer.getReader().size(),
        offset,
        epoch
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
        maxConcurrentWrites
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
    final var futures = new HashMap<Integer, CompletableFuture<AtomicWriteResult>>();
    final var result = new AtomicReference<>(AtomicWriteResult.success(partition));

    try {
      for (final I writer : writers) {
        if (futures.size() >= maxConcurrentWrites) {
          CompletableFuture.anyOf(futures.values().toArray(CompletableFuture[]::new)).get();
          futures.values().removeIf(CompletableFuture::isDone);
        }

        final int subPartition = keyFunction.apply(writer);
        final var writeFuture = action
            .apply(writer)
            .thenApply(r -> setIfNotApplied(result, r))
            .toCompletableFuture();
        futures.put(subPartition, writeFuture);
      }

      // drain any remaining
      for (final var future : futures.values()) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new ProcessorStateException(
          "Failed while flushing partition " + partition + " to remote", e);
    }

    return result.get();
  }

  private static AtomicWriteResult setIfNotApplied(
      final AtomicReference<AtomicWriteResult> other,
      final AtomicWriteResult awr
  ) {
    if (!awr.wasApplied()) {
      other.set(awr);
    }
    return awr;
  }

  private void maybeTruncateChangelog(final long offset) {
    if (truncateChangelog) {
      try {
        admin.deleteRecords(Map.of(changelog, RecordsToDelete.beforeOffset(offset))).all().get();
        log.info("Truncated changelog topic {} before offset {}", changelog, offset);
      } catch (final ExecutionException e) {
        log.warn("Could not truncate changelog topic-partition {}.", changelog, e);
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
    long consumedOffset = -1L;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      consumedOffset = record.offset();
      if (record.value() == null) {
        tombstone(plugin.keyFromRecord(record));
      } else {
        put(plugin.keyFromRecord(record), record.value());
      }
    }

    if (consumedOffset >= 0) {
      final var writeResult = flush(consumedOffset, records.size());
      if (!writeResult.wasApplied()) {
        throwFencedException(writeResult, consumedOffset);
      }
    }
  }

  private void throwFencedException(final AtomicWriteResult result, final long offset) {
    final MetadataRow stored = client.metadata(tableName, result.getPartition());
    // we were fenced - the only conditional statement is the
    // offset update, so it's the only failure point
    final String msg = String.format(
        "%s[%d:%d] Fenced while writing batch! Local Epoch: %d, "
            + "Persisted Epoch: %d, Batch Offset: %d, Persisted Offset: %d",
        tableName,
        partition,
        result.getPartition(),
        epoch,
        stored.epoch,
        offset,
        stored.offset
    );
    log.error(msg);
    throw new TaskMigratedException(msg);
  }

}
