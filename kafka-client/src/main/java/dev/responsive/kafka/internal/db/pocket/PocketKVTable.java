package dev.responsive.kafka.internal.db.pocket;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import dev.responsive.kafka.internal.db.pocket.client.LssId;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.Put;
import dev.responsive.kafka.internal.db.pocket.client.WalEntry;
import java.util.HashMap;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocketKVTable implements RemoteKVTable<WalEntry> {
  private static final Logger LOG = LoggerFactory.getLogger(PocketKVTable.class);

  private final String name;
  private final PocketClient pocketClient;
  private final PssPartitioner pssPartitioner;
  private LssId lssId;
  private long flushedOffset;
  private PocketKVFlushManager flushManager;

  public PocketKVTable(
      final String name,
      final PocketClient pocketClient,
      final PssPartitioner pssPartitioner
  ) {
    this.name = Objects.requireNonNull(name);
    this.pocketClient = Objects.requireNonNull(pocketClient);
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  @Override
  public KVFlushManager init(int kafkaPartition) {
    if (flushManager != null) {
      throw new IllegalStateException("already initialized");
    }
    this.lssId = new LssId(kafkaPartition);
    long flushedOffset = Long.MAX_VALUE;
    final HashMap<Integer, Long> lastWrittenOffset = new HashMap<>();
    for (final int pss: pssPartitioner.allPss()) {
      final var offsets = pocketClient.getCurrentOffsets(lssId, pss);
      flushedOffset = Math.min(flushedOffset, offsets.flushedOffset());
      lastWrittenOffset.put(pss, offsets.writtenOffset());
    }
    if (flushedOffset == this.flushedOffset) {
      throw new IllegalStateException("unable to set flushed offset from remote PSS");
    }
    this.flushedOffset = flushedOffset;
    flushManager = new PocketKVFlushManager(
        pocketClient,
        lssId,
        this,
        lastWrittenOffset,
        kafkaPartition
    );
    return flushManager;
  }

  @Override
  public byte[] get(int kafkaPartition, Bytes key, long minValidTs) {
    final int pssId = pssPartitioner.pss(key.get(), kafkaPartition);
    return pocketClient.get(lssId, pssId, flushManager.writtenOffset(pssId), key.get())
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
    return flushedOffset;
  }
}
