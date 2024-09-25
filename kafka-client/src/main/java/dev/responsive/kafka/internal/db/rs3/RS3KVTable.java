package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
      final String name,
      final UUID storeId,
      final RS3Client rs3Client,
      final PssPartitioner pssPartitioner
  ) {
    this.name = Objects.requireNonNull(name);
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = Objects.requireNonNull(rs3Client);
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  @Override
  public KVFlushManager init(int kafkaPartition) {
    if (flushManager != null) {
      throw new IllegalStateException("already initialized");
    }

    this.lssId = new LssId(kafkaPartition);

    // TODO: we should write an empty segment periodically to any PSS that we haven't
    //       written to to bump the written offset
    final HashMap<Integer, Optional<Long>> lastWrittenOffset = new HashMap<>();
    for (final int pss: pssPartitioner.allPss()) {
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

    LOG.info("restore rs3 kv table from offset {} for {}", fetchOffset, kafkaPartition);

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
  public byte[] get(int kafkaPartition, Bytes key, long minValidTs) {
    final int pssId = pssPartitioner.pss(key.get(), kafkaPartition);
    return rs3Client.get(storeId, lssId, pssId, flushManager.writtenOffset(pssId), key.get())
        .orElse(null);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(int kafkaPartition, Bytes from, Bytes to, long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(int kafkaPartition, long minValidTs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries(int kafkaPartition) {
    LOG.warn("approximateNumEntries not implemented for Pocket");
    return 0;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public WalEntry insert(int kafkaPartition, Bytes key, byte[] value, long epochMillis) {
    // todo: maybe encode epochMillis
    return new Put(
        key.get(),
        value
    );
  }

  @Override
  public WalEntry delete(int kafkaPartition, Bytes key) {
    return new Put(
        key.get(),
        null
    );
  }

  @Override
  public long fetchOffset(int kafkaPartition) {
    return fetchOffset;
  }
}
