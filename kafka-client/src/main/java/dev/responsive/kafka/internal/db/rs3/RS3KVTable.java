/*
 * Copyright 2024 Responsive Computing, Inc.
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

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.MeteredRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.utils.MergeKeyValueIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RS3KVTable implements RemoteKVTable<WalEntry> {
  private static final Logger LOG = LoggerFactory.getLogger(RS3KVTable.class);

  private final String name;
  private final UUID storeId;
  private final RS3Client rs3Client;
  private final PssPartitioner pssPartitioner;
  private LssId lssId;
  private Long fetchOffset = ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
  private RS3KVFlushManager flushManager;

  public RS3KVTable(
      final String storeName,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    this.name = Objects.requireNonNull(storeName);
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = new MeteredRS3Client(
        Objects.requireNonNull(rs3Client),
        Objects.requireNonNull(responsiveMetrics),
        Objects.requireNonNull(scopeBuilder)
    );
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  @Override
  public KVFlushManager init(final int kafkaPartition) {
    if (flushManager != null) {
      LOG.error("already initialized for store {}:{}", name, kafkaPartition);
      throw new IllegalStateException(String.format(
          "already initialized for store %s:%d",
          name,
          kafkaPartition
      ));
    }

    this.lssId = new LssId(kafkaPartition);

    // TODO: we should write an empty segment periodically to any PSS that we haven't
    //       written to to bump the written offset
    final HashMap<Integer, Optional<Long>> lastWrittenOffset = new HashMap<>();
    for (final int pss : pssPartitioner.pssForLss(this.lssId)) {
      final var offsets = rs3Client.getCurrentOffsets(storeId, lssId, pss);
      lastWrittenOffset.put(pss, offsets.writtenOffset());
    }
    final var fetchOffsetOrMinusOne = lastWrittenOffset.values().stream()
        .map(v -> v.orElse(-1L))
        .min(Long::compare)
        .orElse(-1L);
    if (fetchOffsetOrMinusOne == -1) {
      this.fetchOffset = ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
    } else {
      this.fetchOffset = fetchOffsetOrMinusOne;
    }

    final var writtenOffsetsStr = lastWrittenOffset.entrySet().stream()
        .map(e -> String.format("%s -> %s",
            e.getKey(),
            e.getValue().map(Object::toString).orElse("none")))
        .collect(Collectors.joining(","));
    LOG.info("restore rs3 kv table from offset {} for {}. recorded written offsets: {}",
        fetchOffset,
        kafkaPartition,
        writtenOffsetsStr
    );

    flushManager = new RS3KVFlushManager(
        storeId,
        rs3Client,
        lssId,
        this,
        lastWrittenOffset,
        kafkaPartition,
        pssPartitioner
    );
    return flushManager;
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long minValidTs) {
    final int pssId = pssPartitioner.pss(key.get(), this.lssId);
    return rs3Client.get(
        storeId,
        lssId,
        pssId,
        flushManager.writtenOffset(pssId),
        key.get()
    ).orElse(null);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    final var range = new Range(RangeBound.inclusive(from.get()), RangeBound.exclusive(to.get()));
    final List<KeyValueIterator<Bytes, byte[]>> pssIters = new ArrayList<>();

    for (int pssId : pssPartitioner.pssForLss(this.lssId)) {
      pssIters.add(rs3Client.range(
          storeId,
          lssId,
          pssId,
          flushManager.writtenOffset(pssId),
          range
      ));
    }
    return new MergeKeyValueIterator<>(pssIters);
  }

  @Override
  public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefix(
      final P prefix,
      final PS prefixKeySerializer,
      final int kafkaPartition,
      final long streamTimeMs
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    LOG.warn("approximateNumEntries not implemented for RS3");
    return -1;
  }

  @Override
  public String name() {
    return name;
  }

  public UUID storedId() {
    return storeId;
  }

  @Override
  public WalEntry insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long timestampMs
  ) {
    return new Put(
        key.get(),
        value
    );
  }

  @Override
  public WalEntry delete(final int kafkaPartition, final Bytes key) {
    return new Put(
        key.get(),
        null
    );
  }

  @Override
  public long lastWrittenOffset(final int kafkaPartition) {
    return fetchOffset;
  }
}
