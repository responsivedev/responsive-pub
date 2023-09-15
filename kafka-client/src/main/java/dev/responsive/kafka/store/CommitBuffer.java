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
import dev.responsive.db.KeySpec;
import dev.responsive.db.MetadataRow;
import dev.responsive.db.RemoteSchema;
import dev.responsive.db.RemoteWriter;
import dev.responsive.db.WriterFactory;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

class CommitBuffer<K, S extends RemoteSchema<K>> implements RecordBatchingStateRestoreCallback {

  public static final int MAX_BATCH_SIZE = 1000;
  private final Logger log;

  private final SizeTrackingBuffer<K> buffer;
  private final CassandraClient client;
  private final String tableName;
  private final int partition;
  private final Admin admin;
  private final TopicPartition changelog;
  private final S remoteSchema;
  private final boolean truncateChangelog;
  private final FlushTriggers flushTriggers;
  private final int maxBatchSize;
  private final SubPartitioner subPartitioner;
  private final Supplier<Instant> clock;
  private final KeySpec<K> keySpec;

  private Instant lastFlush;
  private WriterFactory<K> writerFactory;

  // flag to skip further truncation attempts when the changelog is set to 'compact' only
  private boolean isNotDeleteEnabled = false;

  static <K, S extends RemoteSchema<K>> CommitBuffer<K, S> from(
      final SharedClients clients,
      final TableName tableName,
      final TopicPartition changelog,
      final S schema,
      final KeySpec<K> keySpec,
      final boolean truncateChangelog,
      final SubPartitioner partitioner,
      final ResponsiveConfig config
  ) {
    final var admin = clients.admin;
    final var cassandraClient = clients.cassandraClient;

    return new CommitBuffer<>(
        cassandraClient,
        tableName.cassandraName(),
        changelog,
        admin,
        schema,
        keySpec,
        truncateChangelog,
        FlushTriggers.fromConfig(config),
        partitioner
    );
  }

  CommitBuffer(
      final CassandraClient client,
      final String tableName,
      final TopicPartition changelog,
      final Admin admin,
      final S schema,
      final KeySpec<K> keySpec,
      final boolean truncateChangelog,
      final FlushTriggers flushTriggers,
      final SubPartitioner subPartitioner
  ) {
    this(
        client,
        tableName,
        changelog,
        admin,
        schema,
        keySpec,
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
      final S schema,
      final KeySpec<K> keySpec,
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
    this.remoteSchema = schema;

    this.buffer = new SizeTrackingBuffer<>(keySpec);
    this.keySpec = keySpec;
    this.truncateChangelog = truncateChangelog;
    this.flushTriggers = flushTriggers;
    this.maxBatchSize = maxBatchSize;
    this.subPartitioner = subPartitioner;
    this.clock = clock;

    final String logPrefix = String.format("commit-buffer-%s[%d] ", tableName, partition);
    final LogContext logContext = new LogContext(logPrefix);
    log = logContext.logger(CommitBuffer.class);
  }

  public void init() {
    lastFlush = clock.get();

    this.writerFactory = remoteSchema.init(tableName, subPartitioner, partition);

    final int basePartition = subPartitioner.first(partition);
    log.info("Initialized store with {} for all subpartitions {} -> {}",
        writerFactory, basePartition, basePartition + subPartitioner.getFactor() - 1);
  }

  public void put(final K key, final byte[] value, long timestamp) {
    buffer.put(key, Result.value(key, value, timestamp));
  }

  public void tombstone(final K key, long timestamp) {
    buffer.put(key, Result.tombstone(key, timestamp));
  }

  public Result<K> get(final K key) {
    final Result<K> result = buffer.getReader().get(key);
    if (result != null && keySpec.retain(result.key)) {
      return result;
    }
    return null;
  }

  public KeyValueIterator<K, Result<K>> range(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().subMap(from, to).entrySet().iterator(),
            e -> keySpec.retain(e.getKey())),
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
            e -> keySpec.retain(e.getKey()) && filter.test(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backRange(final K from, final K to) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().descendingMap().subMap(to, from).entrySet().iterator(),
            e -> keySpec.retain(e.getKey())
        ),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> all(
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().entrySet().iterator(),
            e -> keySpec.retain(e.getKey())
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
            kv -> keySpec.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  public KeyValueIterator<K, Result<K>> backAll(
      final Predicate<K> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().descendingMap().entrySet().iterator(),
            kv -> keySpec.retain(kv.getKey()) && filter.test(kv.getKey())),
        result -> new KeyValue<>(result.getKey(), result.getValue())
    );
  }

  long offset() {
    final int basePartition = subPartitioner.first(partition);
    return remoteSchema.metadata(tableName, basePartition).offset;
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
  private RemoteWriteResult flush(final long offset, final int batchSize) {
    final long startNs = System.nanoTime();
    log.info("Flushing {} records to remote (offset={}, writer={})",
        buffer.getReader().size(),
        offset,
        writerFactory
    );

    final var writers = new HashMap<Integer, RemoteWriter<K>>();
    for (final Result<K> result : buffer.getReader().values()) {
      final int subPartition = subPartitioner.partition(partition, keySpec.bytes(result.key));
      final RemoteWriter<K> writer = writers
          .computeIfAbsent(subPartition, k -> writerFactory.createWriter(
              client,
              tableName,
              subPartition,
              batchSize
          ));

      if (result.isTombstone) {
        writer.delete(result.key);
      } else if (keySpec.retain(result.key)) {
        writer.insert(result.key, result.value, result.timestamp);
      }
    }

    var writeResult = drain(writers.values());
    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    // this offset is only used for recovery, so it can (and should) be done only
    // when all the flushes above have completed and only needs to be written to
    // the first subpartition
    writeResult = writers.computeIfAbsent(
        subPartitioner.first(partition),
        partition -> writerFactory.createWriter(client, tableName, partition, batchSize)
    ).setOffset(offset);

    if (!writeResult.wasApplied()) {
      return writeResult;
    }

    log.info("Flushed {} records to remote in {}ms (offset={}, writer={}, numPartitions={})",
        buffer.getReader().size(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs),
        offset,
        writerFactory,
        writers.size()
    );
    buffer.clear();

    maybeTruncateChangelog(offset);
    return RemoteWriteResult.success(partition);
  }

  private RemoteWriteResult drain(final Collection<RemoteWriter<K>> writers) {
    var result = CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    for (final RemoteWriter<K> writer : writers) {
      result = result.thenCombine(writer.flush(), (one, two) -> !one.wasApplied() ? one : two);
    }

    try {
      return result.toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(
          "Failed while flushing partition " + partition + " to remote", e);
    }
  }

  private void maybeTruncateChangelog(final long offset) {
    if (truncateChangelog && !isNotDeleteEnabled) {
      try {
        admin.deleteRecords(Map.of(changelog, RecordsToDelete.beforeOffset(offset))).all().get();
        log.info("Truncated changelog topic {} before offset {}", changelog, offset);
      } catch (final ExecutionException e) {
        log.warn("Could not truncate changelog topic-partition " + changelog, e);
        if (e.getCause() instanceof PolicyViolationException) {
          log.info("Disabling further changelog truncation attempts due to topic configuration "
                       + "being incompatible with deleteRecord requests", e);
          isNotDeleteEnabled = true;
        }
      } catch (final InterruptedException e) {
        log.error("Interrupted while truncating the changelog " + changelog, e);
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
        tombstone(keySpec.keyFromRecord(record), record.timestamp());
      } else {
        put(keySpec.keyFromRecord(record), record.value(), record.timestamp());
      }
    }

    if (consumedOffset >= 0) {
      final var writeResult = flush(consumedOffset, records.size());
      if (!writeResult.wasApplied()) {
        throwFencedException(writeResult, consumedOffset);
      }
    }
  }

  private void throwFencedException(final RemoteWriteResult result, final long offset) {
    final MetadataRow stored = remoteSchema.metadata(tableName, result.getPartition());
    // we were fenced - the only conditional statement is the
    // offset update, so it's the only failure point
    final String msg = String.format(
        "%s[%d:%d] Fenced while writing batch! Local Epoch: %s, "
            + "Persisted Epoch: %d, Batch Offset: %d, Persisted Offset: %d",
        tableName,
        partition,
        result.getPartition(),
        writerFactory,
        stored.epoch,
        offset,
        stored.offset
    );
    log.error(msg);
    throw new TaskMigratedException(msg);
  }

}
