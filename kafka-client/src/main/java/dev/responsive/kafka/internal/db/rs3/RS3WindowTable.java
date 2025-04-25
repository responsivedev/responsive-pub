/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.LssMetadata;
import dev.responsive.kafka.internal.db.rs3.client.MeteredRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3ClientUtil;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.MergeKeyValueIterator;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RS3WindowTable implements RemoteWindowTable<WalEntry> {
  private static final Logger LOG = LoggerFactory.getLogger(RS3WindowTable.class);

  private final String name;
  private final UUID storeId;
  private final RS3Client rs3Client;
  private final RS3ClientUtil rs3ClientUtil;
  private final PssPartitioner pssPartitioner;

  // Initialized in `init()`
  private LssId lssId;
  private Long fetchOffset;
  private RS3WindowFlushManager flushManager;

  public RS3WindowTable(
      final String name,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    this(
        name,
        storeId,
        new MeteredRS3Client(
            Objects.requireNonNull(rs3Client),
            Objects.requireNonNull(responsiveMetrics),
            Objects.requireNonNull(scopeBuilder)
        ),
        pssPartitioner
    );
  }

  // Visible for testing
  RS3WindowTable(
      final String name,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner
  ) {
    this.name = Objects.requireNonNull(name);
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = Objects.requireNonNull(rs3Client);
    this.rs3ClientUtil = new RS3ClientUtil(storeId, rs3Client, pssPartitioner);
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  public UUID storeId() {
    return storeId;
  }

  @Override
  public WindowFlushManager<Integer> init(final int kafkaPartition) {
    if (flushManager != null) {
      LOG.error("already initialized for store {}:{}", name, kafkaPartition);
      throw new IllegalStateException(String.format(
          "already initialized for store %s:%d",
          name,
          kafkaPartition
      ));
    }

    this.lssId = new LssId(kafkaPartition);
    LssMetadata lssMetadata = rs3ClientUtil.fetchLssMetadata(lssId);
    this.fetchOffset = lssMetadata.lastWrittenOffset();

    final var initialStreamTime = -1L; // TODO: Initialize from RS3 metadata?
    this.flushManager = new RS3WindowFlushManager(
        storeId,
        rs3Client,
        lssId,
        this,
        kafkaPartition,
        pssPartitioner,
        lssMetadata.writtenOffsets(),
        initialStreamTime
    );
    return flushManager;
  }

  private void throwIfPartitionNotInitialized(final int kafkaPartition) {
    if (flushManager == null) {
      throw new IllegalStateException(String.format(
          "Cannot complete operation on store %s is not yet initialized",
          name
      ));
    } else if (lssId.id() != kafkaPartition) {
      throw new IllegalStateException(String.format(
          "Cannot complete operation on store %s for kafka partition %d since the store "
              + "was initialized for a separate partition %d",
          name,
          kafkaPartition,
          lssId.id()
      ));
    }
  }

  @Override
  public byte[] fetch(
      final int kafkaPartition,
      final Bytes key,
      final long windowStart
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    final int pssId = pssPartitioner.pss(key.get(), this.lssId);
    final var windowKey = new WindowedKey(key, windowStart);
    return rs3Client.windowedGet(
        storeId,
        lssId,
        pssId,
        flushManager.writtenOffset(pssId),
        windowKey
    ).orElse(null);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    final int pssId = pssPartitioner.pss(key.get(), this.lssId);
    final var windowRange = new Range<>(
        RangeBound.inclusive(new WindowedKey(key, timeFrom)),
        RangeBound.exclusive(new WindowedKey(key, timeTo))
    );
    return rs3Client.windowedRange(
        storeId,
        lssId,
        pssId,
        flushManager.writtenOffset(pssId),
        windowRange
    );
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetch(
      final int kafkaPartition,
      final Bytes key,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    final List<KeyValueIterator<WindowedKey, byte[]>> pssIters = new ArrayList<>();
    final var windowRange = new Range<>(
        RangeBound.inclusive(new WindowedKey(fromKey, timeFrom)),
        RangeBound.exclusive(new WindowedKey(toKey, timeTo))
    );

    for (int pssId : pssPartitioner.pssForLss(this.lssId)) {
      pssIters.add(rs3Client.windowedRange(
          storeId,
          lssId,
          pssId,
          flushManager.writtenOffset(pssId),
          windowRange
      ));
    }
    return new MergeKeyValueIterator<>(pssIters);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchRange(
      final int kafkaPartition,
      final Bytes fromKey,
      final Bytes toKey,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> fetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    final List<KeyValueIterator<WindowedKey, byte[]>> pssIters = new ArrayList<>();

    // TODO: the types break down here. We want to tell the server that we are
    //  interested in all keys within a given time range, but the schema does
    //  not support a partially specified bound. For now, we fetch everything and
    //  filter here.
    final var timeRange = new Range<>(
        RangeBound.inclusive(timeFrom),
        RangeBound.exclusive(timeTo)
    );

    for (int pssId : pssPartitioner.pssForLss(this.lssId)) {
      final var rangeIter = rs3Client.windowedRange(
          storeId,
          lssId,
          pssId,
          flushManager.writtenOffset(pssId),
          Range.unbounded()
      );
      pssIters.add(new TimeRangeFilter(timeRange, rangeIter));
    }
    return new MergeKeyValueIterator<>(pssIters);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> backFetchAll(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    throwIfPartitionNotInitialized(kafkaPartition);
    throw new UnsupportedOperationException();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WalEntry insert(
      final int kafkaPartition,
      final WindowedKey key,
      final byte[] value,
      final long timestampMs
  ) {
    return new WindowedPut(
        key.key.get(),
        value,
        timestampMs,
        key.windowStartMs
    );
  }

  @Override
  public WalEntry delete(final int kafkaPartition, final WindowedKey key) {
    return new WindowedDelete(
        key.key.get(),
        key.windowStartMs
    );
  }

  @Override
  public long lastWrittenOffset(final int kafkaPartition) {
    return fetchOffset;
  }

  private static class TimeRangeFilter implements KeyValueIterator<WindowedKey, byte[]> {
    private final Range<Long> timeRange;
    private final KeyValueIterator<WindowedKey, byte[]> delegate;

    private TimeRangeFilter(
        final Range<Long> timeRange,
        final KeyValueIterator<WindowedKey, byte[]> delegate
    ) {
      this.timeRange = timeRange;
      this.delegate = delegate;
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public WindowedKey peekNextKey() {
      skipToNextKeyInRange();
      return delegate.peekNextKey();
    }

    private void skipToNextKeyInRange() {
      while (true) {
        final var nextKey = delegate.peekNextKey();
        if (nextKey == null) {
          break;
        } else if (timeRange.contains(nextKey.windowStartMs)) {
          return;
        } else {
          delegate.next();
        }
      }
    }

    @Override
    public boolean hasNext() {
      return peekNextKey() != null;
    }

    @Override
    public KeyValue<WindowedKey, byte[]> next() {
      skipToNextKeyInRange();
      return delegate.next();
    }
  }

}
