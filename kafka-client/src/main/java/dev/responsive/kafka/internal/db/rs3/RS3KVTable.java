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
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.LssMetadata;
import dev.responsive.kafka.internal.db.rs3.client.MeteredRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3ClientUtil;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.utils.MergeKeyValueIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
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
  private final RS3ClientUtil rs3ClientUtil;
  private final PssPartitioner pssPartitioner;
  private LssId lssId;
  private Long fetchOffset = ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;
  private RS3KVFlushManager flushManager;

  public RS3KVTable(
      final String name,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner,
      final ResponsiveMetrics responsiveMetrics,
      final ResponsiveMetrics.MetricScopeBuilder scopeBuilder
  ) {
    this.name = Objects.requireNonNull(name);
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = new MeteredRS3Client(
        Objects.requireNonNull(rs3Client),
        Objects.requireNonNull(responsiveMetrics),
        Objects.requireNonNull(scopeBuilder)
    );
    this.rs3ClientUtil = new RS3ClientUtil(storeId, rs3Client, pssPartitioner);
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

    LssMetadata lssMetadata = rs3ClientUtil.fetchLssMetadata(lssId);
    this.fetchOffset = lssMetadata.lastWrittenOffset();
    this.flushManager = new RS3KVFlushManager(
        storeId,
        rs3Client,
        lssId,
        this,
        lssMetadata.writtenOffsets(),
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
        key
    ).orElse(null);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    final RangeBound<Bytes> fromBound = RangeBound.inclusive(from);
    final RangeBound<Bytes> toBound = RangeBound.exclusive(to);
    final List<KeyValueIterator<Bytes, byte[]>> pssIters = new ArrayList<>();

    for (int pssId : pssPartitioner.pssForLss(this.lssId)) {
      pssIters.add(rs3Client.range(
          storeId,
          lssId,
          pssId,
          flushManager.writtenOffset(pssId),
          fromBound,
          toBound
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
    return new Delete(key.get());
  }

  @Override
  public long lastWrittenOffset(final int kafkaPartition) {
    return fetchOffset;
  }
}
