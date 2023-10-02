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

package dev.responsive.kafka.internal.stores;

import static org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.db.MetadataRow;
import dev.responsive.kafka.internal.db.RemoteSchema;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.WriterFactory;
import dev.responsive.kafka.internal.db.partitioning.SubPartitioner;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SharedClients;
import dev.responsive.kafka.internal.utils.TableName;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

class CommitBuffer<K, S extends RemoteSchema<K>> 
    implements RecordBatchingStateRestoreCallback, Closeable {

  public static final int MAX_BATCH_SIZE = 1000;

  private final Logger log;
  private final String logPrefix;

  private final SizeTrackingBuffer<K> buffer;
  private final CassandraClient client;
  private final String tableName;
  private final int partition;
  private final Admin admin;
  private final TopicPartition changelog;
  private final S remoteSchema;
  private final FlushTriggers flushTriggers;
  private final int maxBatchSize;
  private final SubPartitioner subPartitioner;
  private final Supplier<Instant> clock;
  private final KeySpec<K> keySpec;

  private Instant lastFlush;
  private WriterFactory<K> writerFactory;

  // flag to skip further truncation attempts when the changelog is set to 'compact' only
  private boolean isDeleteEnabled = true;
  private final boolean truncateChangelog;
  private KafkaFuture<DeletedRecords> deleteRecordsFuture;

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
    this.flushTriggers = flushTriggers;
    this.maxBatchSize = maxBatchSize;
    this.subPartitioner = subPartitioner;
    this.clock = clock;

    logPrefix = String.format("commit-buffer [%s-%d] ", tableName, partition);
    log = new LogContext(logPrefix).logger(CommitBuffer.class);

    if (hasSourceTopicChangelog(changelog.topic())) {
      this.truncateChangelog = false;
      if (truncateChangelog) {
        log.warn("Changelog truncation is not compatible with the source-topic changelog "
                     + "optimization, and will not be enabled for the topic {}", changelog.topic());
      }
    } else {
      this.truncateChangelog = truncateChangelog;
    }
  }

  private static boolean hasSourceTopicChangelog(final String changelogTopicName) {
    return !changelogTopicName.endsWith("changelog");
  }

  public void init() {
    lastFlush = clock.get();

    this.writerFactory = remoteSchema.init(tableName, subPartitioner, partition);

    final int basePartition = subPartitioner.first(partition);
    log.info("Initialized store with {} for subpartitions in range: {{} -> {}}",
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

  private long totalBytesBuffered() {
    return buffer.getBytes();
  }

  private boolean triggerFlush() {
    boolean recordsTrigger = false;
    boolean bytesTrigger = false;
    boolean timeTrigger = false;
    if (buffer.getReader().size() >= flushTriggers.getRecords()) {
      log.debug("Will flush due to records buffered {} over trigger {}",
          buffer.getReader().size(),
          flushTriggers.getRecords()
      );
      recordsTrigger = true;
    } else {
      log.debug("Records buffered since last flush {} not over trigger {}",
          buffer.getReader().size(),
          flushTriggers.getRecords()
      );
    }
    final long totalBytesBuffered = totalBytesBuffered();
    if (totalBytesBuffered >= flushTriggers.getBytes()) {
      log.debug("Will flush due to bytes buffered {} over bytes trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
      bytesTrigger = true;
    } else {
      log.debug("Bytes buffered since last flush {} not over trigger {}",
          totalBytesBuffered,
          flushTriggers.getBytes()
      );
    }
    final var now = clock.get();
    if (lastFlush.plus(flushTriggers.getInterval()).isBefore(now)) {
      log.debug("Will flush as time since last flush {} over interval trigger {}",
          Duration.between(lastFlush, now),
          now
      );
      timeTrigger = true;
    } else {
      log.debug("Time since last flush {} not over trigger {}",
          Duration.between(lastFlush, now),
          now
      );
    }
    return recordsTrigger || bytesTrigger || timeTrigger;
  }

  public void flush(final long consumedOffset) {
    if (!triggerFlush()) {
      return;
    }

    if (buffer.getReader().isEmpty()) {
      log.debug("Ignoring flush() of empty commit buffer");
      return;
    }

    doFlush(consumedOffset, maxBatchSize);

    lastFlush = clock.get();
  }

  private void doFlush(final long consumedOffset, final int batchSize) {
    final long startNs = System.nanoTime();
    log.info("Flushing {} records with batchSize={} to remote (offset={}, writer={})",
        buffer.getReader().size(),
        batchSize,
        consumedOffset,
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

    final var drainWriteResult = drain(writers.values());
    if (!drainWriteResult.wasApplied()) {
      throwFencedException(drainWriteResult, consumedOffset);
    }

    // this offset is only used for recovery, so it can (and should) be done only
    // when all the flushes above have completed and only needs to be written to
    // the first subpartition
    final var offsetWriteResult = writers.computeIfAbsent(
        subPartitioner.first(partition),
        subPartition -> writerFactory.createWriter(client, tableName, subPartition, batchSize)
    ).setOffset(consumedOffset);

    if (!offsetWriteResult.wasApplied()) {
      throwFencedException(offsetWriteResult, consumedOffset);
    }

    log.debug("Flushed {} records to remote in {}ms (offset={}, writer={}, numSubPartitions={})",
        buffer.getReader().size(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs),
        consumedOffset,
        writerFactory,
        writers.size()
    );
    buffer.clear();

    maybeTruncateChangelog(consumedOffset);
  }

  private RemoteWriteResult drain(final Collection<RemoteWriter<K>> writers) {
    var result = CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    for (final RemoteWriter<K> writer : writers) {
      result = result.thenCombine(writer.flush(), (one, two) -> !one.wasApplied() ? one : two);
    }

    try {
      return result.toCompletableFuture().get();
    } catch (final InterruptedException | ExecutionException e) {
      log.error("Unexpected exception while flushing to remote", e);
      throw new RuntimeException(logPrefix + "Failed while flushing to remote", e);
    }
  }

  /**
   * Performs async changelog truncation by allowing the delete requests to be completed
   * in the background. After a flush, we will check on the completion of the previous
   * delete request and if successful, send out a new request to truncate up to the
   * last flushed offset
   *
   * @param offset the flushed offset up to which we can safely delete changelog records
   */
  private void maybeTruncateChangelog(final long offset) {
    if (truncateChangelog && isDeleteEnabled) {
      if (deleteRecordsFuture.isDone()) {
        log.debug("Issuing new delete request to truncate {} up to offset {}", changelog, offset);

        deleteRecordsFuture = admin.deleteRecords(Map.of(changelog, beforeOffset(offset)))
            .lowWatermarks()
            .get(changelog)
            .whenComplete(this::onDeleteRecords);
      } else {
        log.debug("Still waiting on previous changelog truncation attempt to complete");
      }
    }
  }

  private void onDeleteRecords(final DeletedRecords deletedRecords, final Throwable throwable) {
    if (throwable == null) {
      log.info("Truncated changelog {} up to offset {}", changelog, deletedRecords.lowWatermark());
    } else {
      if (throwable instanceof PolicyViolationException) {
        // Don't retry and cancel all further attempts since we know they will fail
        log.warn("Disabling further changelog truncation attempts due to topic configuration "
                     + "being incompatible with deleteRecords requests", throwable);
        isDeleteEnabled = false;
      } else if (throwable instanceof CancellationException) {
        // Don't retry and just log at INFO since we cancel the future as part of shutdown
        log.info("Delete records request for changelog {} was cancelled", changelog);
      } else {
        // Pretty much anything else can and should be retried, but we always wait for the
        // next flush to retry to make sure the offset gets updated
        log.warn("Truncation of changelog " + changelog + " failed and will be retried"
                     + "after the next flush", throwable);
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
      doFlush(consumedOffset, records.size());
    }
  }

  private void throwFencedException(final RemoteWriteResult result, final long consumedOffset) {
    final MetadataRow stored = remoteSchema.metadata(tableName, result.getPartition());
    // we were fenced - the only conditional statement is the
    // offset update, so it's the only failure point
    final String msg = String.format(
        "[%d] Fenced while writing batch! Local Epoch: %s, "
            + "Persisted Epoch: %d, Batch Offset: %d, Persisted Offset: %d",
        result.getPartition(),
        writerFactory,
        stored.epoch,
        consumedOffset,
        stored.offset
    );
    log.warn(msg);
    throw new TaskMigratedException(logPrefix + msg);
  }

  @Override
  public void close() {
    deleteRecordsFuture.cancel(true);
  }
}
