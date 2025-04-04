package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.RemoteWindowTable;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.LssMetadata;
import dev.responsive.kafka.internal.db.rs3.client.MeteredRS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3ClientUtil;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
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
  private final RS3WindowedKeySerde keySerde = new RS3WindowedKeySerde();

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

  public RS3WindowTable(
      final String name,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner
  ) {
    this.name = Objects.requireNonNull(name);
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = Objects.requireNonNull(rs3Client); // TODO: Use metered client
    this.rs3ClientUtil = new RS3ClientUtil(storeId, rs3Client, pssPartitioner);
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
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
        keySerde,
        lssMetadata.writtenOffsets(),
        initialStreamTime
    );
    return flushManager;
  }

  private void throwIfPartitionNotInitialized(int kafkaPartition) {
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
    final var windowKeyBytes = keySerde.serialize(windowKey);

    return rs3Client.get(
        storeId,
        lssId,
        pssId,
        flushManager.writtenOffset(pssId),
        windowKeyBytes
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    return new Put(keySerde.serialize(key), value);
  }

  @Override
  public WalEntry delete(final int kafkaPartition, final WindowedKey key) {
    return new Put(keySerde.serialize(key), null);
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    return fetchOffset;
  }
}
