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

import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FAILED_TRUNCATIONS_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_ERRORS_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_AVG;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_AVG_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_MAX;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_LATENCY_MAX_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_RATE;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_RATE_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_TOTAL;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.FLUSH_TOTAL_DESCRIPTION;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_SINCE_LAST_FLUSH;
import static dev.responsive.kafka.internal.metrics.StoreMetrics.TIME_SINCE_LAST_FLUSH_DESCRIPTION;
import static org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.BatchFlusher;
import dev.responsive.kafka.internal.db.BatchFlusher.FlushResult;
import dev.responsive.kafka.internal.db.KeySpec;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.ExceptionSupplier;
import dev.responsive.kafka.internal.utils.Iterators;
import dev.responsive.kafka.internal.utils.Result;
import dev.responsive.kafka.internal.utils.SessionClients;
import dev.responsive.kafka.internal.utils.TableName;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;

public class CommitBuffer<K extends Comparable<K>, P>
    implements RecordBatchingStateRestoreCallback, Closeable {

  public static final int MAX_BATCH_SIZE = 1000;

  private final Logger log;
  private final String logPrefix;

  private final BatchFlusher<K, P> batchFlusher;
  private final SizeTrackingBuffer<K> buffer;
  private final ResponsiveMetrics metrics;
  private final Admin admin;
  private final TopicPartition changelog;
  private final FlushTriggers flushTriggers;
  private final ExceptionSupplier exceptionSupplier;
  private final int maxBatchSize;
  private final Supplier<Instant> clock;
  private final KeySpec<K> keySpec;

  private final String flushSensorName;
  private final String flushLatencySensorName;
  private final String flushErrorsSensorName;
  private final String failedTruncationsSensorName;

  private final MetricName lastFlushMetric;
  private final Sensor flushSensor;
  private final Sensor flushLatencySensor;
  private final Sensor flushErrorsSensor;
  private final Sensor failedTruncationsSensor;

  // flag to skip further truncation attempts when the changelog is set to 'compact' only
  private boolean isDeleteEnabled = true;
  private final boolean truncateChangelog;
  private KafkaFuture<DeletedRecords> deleteRecordsFuture = KafkaFuture.completedFuture(null);

  private Instant lastFlush;

  static <K extends Comparable<K>, P> CommitBuffer<K, P> from(
      final BatchFlusher<K, P> batchFlusher,
      final SessionClients clients,
      final TopicPartition changelog,
      final KeySpec<K> keySpec,
      final boolean truncateChangelog,
      final TableName tableName,
      final ResponsiveConfig config
  ) {
    final var admin = clients.admin();

    return new CommitBuffer<>(
        batchFlusher,
        clients,
        changelog,
        admin,
        keySpec,
        truncateChangelog,
        tableName,
        FlushTriggers.fromConfig(config),
        ExceptionSupplier.fromConfig(config.originals())
    );
  }

  CommitBuffer(
      final BatchFlusher<K, P> batchFlusher,
      final SessionClients sessionClients,
      final TopicPartition changelog,
      final Admin admin,
      final KeySpec<K> keySpec,
      final boolean truncateChangelog,
      final TableName tableName,
      final FlushTriggers flushTriggers,
      final ExceptionSupplier exceptionSupplier
  ) {
    this(
        batchFlusher,
        sessionClients,
        changelog,
        admin,
        keySpec,
        truncateChangelog,
        tableName,
        flushTriggers,
        exceptionSupplier,
        MAX_BATCH_SIZE,
        Instant::now
    );
  }

  CommitBuffer(
      final BatchFlusher<K, P> batchFlusher,
      final SessionClients sessionClients,
      final TopicPartition changelog,
      final Admin admin,
      final KeySpec<K> keySpec,
      final boolean truncateChangelog,
      final TableName tableName,
      final FlushTriggers flushTriggers,
      final ExceptionSupplier exceptionSupplier,
      final int maxBatchSize,
      final Supplier<Instant> clock
  ) {
    this.batchFlusher = batchFlusher;
    this.changelog = changelog;
    this.metrics = sessionClients.metrics();
    this.admin = admin;

    this.buffer = new SizeTrackingBuffer<>(keySpec);
    this.keySpec = keySpec;
    this.flushTriggers = flushTriggers;
    this.exceptionSupplier = exceptionSupplier;
    this.maxBatchSize = maxBatchSize;
    this.clock = clock;
    this.lastFlush = clock.get();

    logPrefix = String.format("commit-buffer [%s-%d] ",
                              tableName.tableName(), changelog.partition());
    log = new LogContext(logPrefix).logger(CommitBuffer.class);

    final String storeName = tableName.kafkaName();

    flushSensorName = getSensorName(FLUSH, changelog);
    flushLatencySensorName = getSensorName(FLUSH_LATENCY, changelog);
    flushErrorsSensorName = getSensorName(FLUSH_ERRORS, changelog);
    failedTruncationsSensorName = getSensorName(FAILED_TRUNCATIONS, changelog);

    lastFlushMetric = metrics.metricName(
        TIME_SINCE_LAST_FLUSH,
        TIME_SINCE_LAST_FLUSH_DESCRIPTION,
        metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)
    );
    metrics.addMetric(
        lastFlushMetric,
        (Gauge<Long>) (config, now) -> now - lastFlush.toEpochMilli()
    );

    flushSensor = metrics.addSensor(flushSensorName);
    flushSensor.add(
        metrics.metricName(
            FLUSH_RATE,
            FLUSH_RATE_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new Rate()
    );
    flushSensor.add(
        metrics.metricName(
            FLUSH_TOTAL,
            FLUSH_TOTAL_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new CumulativeCount()
    );

    flushLatencySensor = metrics.addSensor(flushLatencySensorName);
    flushLatencySensor.add(
        metrics.metricName(
            FLUSH_LATENCY_AVG,
            FLUSH_LATENCY_AVG_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new Avg()
    );
    flushLatencySensor.add(
        metrics.metricName(
            FLUSH_LATENCY_MAX,
            FLUSH_LATENCY_MAX_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new Max()
    );

    flushErrorsSensor = metrics.addSensor(flushErrorsSensorName);
    flushErrorsSensor.add(
        metrics.metricName(
            FLUSH_ERRORS_RATE,
            FLUSH_ERRORS_RATE_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new Rate()
    );
    flushErrorsSensor.add(
        metrics.metricName(
            FLUSH_ERRORS_TOTAL,
            FLUSH_ERRORS_TOTAL_DESCRIPTION,
            metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
        new CumulativeCount()
    );

    failedTruncationsSensor = metrics.addSensor(failedTruncationsSensorName);

    if (hasSourceTopicChangelog(changelog.topic())) {
      this.truncateChangelog = false;
      if (truncateChangelog) {
        log.warn("Changelog truncation is not compatible with the source-topic changelog "
                     + "optimization, and will not be enabled for the topic {}", changelog.topic());
      }
    } else {
      this.truncateChangelog = truncateChangelog;

      if (truncateChangelog) {
        failedTruncationsSensor.add(
            metrics.metricName(
                FAILED_TRUNCATIONS_RATE,
                FAILED_TRUNCATIONS_RATE_DESCRIPTION,
                metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
            new Rate()
        );
        failedTruncationsSensor.add(
            metrics.metricName(
                FAILED_TRUNCATIONS_TOTAL,
                FAILED_TRUNCATIONS_TOTAL_DESCRIPTION,
                metrics.storeLevelMetric(metrics.computeThreadId(), changelog, storeName)),
            new CumulativeCount()
        );
      }
    }
  }

  // Attach the changelog topic name & partition to make sure we uniquely name each sensor
  private static String getSensorName(final String sensorPrefix, final TopicPartition changelog) {
    return sensorPrefix + "-" + changelog;
  }

  private static boolean hasSourceTopicChangelog(final String changelogTopicName) {
    return !changelogTopicName.endsWith("changelog");
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

  // TODO: remove all of the below methods that include a filter param, since
  //  the predicate should be the same every time and could be consolidated
  //  with the keySpec similar to keySpec#retain, for example
  //  In addition to just reducing the API surface area and making things more
  //  ergonomic by only having to define the filter predicate once, without
  //  this fix the current system is highly error-prone, mainly because it is
  //  easy to accidentally invoke one of the identical overloads that don't
  //  accept a filter parameter by mistake
  public Result<K> get(final K key, final Predicate<Result<K>> filter) {
    final Result<K> result = buffer.getReader().get(key);
    if (result != null && keySpec.retain(result.key) && filter.test(result)) {
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
      final Predicate<Result<K>> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().subMap(from, to).entrySet().iterator(),
            e -> keySpec.retain(e.getKey()) && filter.test(e.getValue())
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
      final Predicate<Result<K>> filter
  ) {
    return Iterators.kv(
        Iterators.filter(
            buffer.getReader().entrySet().iterator(),
            kv -> keySpec.retain(kv.getKey()) && filter.test(kv.getValue())),
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
             batchFlusher
    );

    final FlushResult<K, P> flushResult = batchFlusher.flushWriteBatch(
        buffer.getReader(), consumedOffset
    );

    if (!flushResult.result().wasApplied()) {
      final P failedTablePartition = flushResult.result().tablePartition();
      log.warn("Error while flushing batch for table partition {}", failedTablePartition);

      flushErrorsSensor.record();

      final String errorMsg = String.format(
          "Failed table partition [%s]: %s",
          failedTablePartition, flushResult.failedFlushInfo(consumedOffset, failedTablePartition)
      );
      log.warn(errorMsg);

      throw exceptionSupplier.commitFencedException(logPrefix + errorMsg);
    }

    final long endNanos = System.nanoTime();
    final long endMs = TimeUnit.NANOSECONDS.toMillis(endNanos);
    final long flushLatencyMs = TimeUnit.NANOSECONDS.toMillis(endNanos - startNs);

    flushSensor.record(1, endMs);
    flushLatencySensor.record(flushLatencyMs, endMs);

    log.info("Flushed {} records to {} table partitions with offset {} in {}ms",
             buffer.getReader().size(),
             flushResult.numTablePartitionsFlushed(),
             consumedOffset,
             flushLatencyMs
    );
    buffer.clear();

    maybeTruncateChangelog(consumedOffset);
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
      failedTruncationsSensor.record();

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

  @Override
  public void close() {
    deleteRecordsFuture.cancel(true);
    metrics.removeMetric(lastFlushMetric);
    metrics.removeSensor(flushSensorName);
    metrics.removeSensor(flushLatencySensorName);
    metrics.removeSensor(flushErrorsSensorName);
    metrics.removeSensor(failedTruncationsSensorName);
  }
}
