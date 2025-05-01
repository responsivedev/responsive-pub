package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.kafka.internal.db.rs3.client.jni.JNIRs3ReaderClient;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class RS3ReadOnlyKeyValueBytesStore implements RS3ReadOnlyKeyValueStore<Bytes, byte[]> {
  private final RS3ReaderClient readerClient;
  private final UUID storeReaderId;
  private final LssId lssId;
  private final int pssId;

  public RS3ReadOnlyKeyValueBytesStore(
      final RS3ReaderClient readerClient,
      final UUID storeReaderId,
      final int partition
  ) {
    this.readerClient = Objects.requireNonNull(readerClient);
    this.storeReaderId = Objects.requireNonNull(storeReaderId);
    this.lssId = new LssId(partition);
    this.pssId = partition;
  }

  @Override
  public byte[] get(Bytes key) {
    return readerClient.get(storeReaderId, lssId, pssId, Optional.empty(), key)
        .orElse(null);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    return readerClient.range(
        storeReaderId,
        lssId,
        pssId,
        Optional.empty(),
        new Range<>(RangeBound.inclusive(from), RangeBound.inclusive(to))
    );
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return readerClient.range(
        storeReaderId,
        lssId,
        pssId,
        Optional.empty(),
        Range.unbounded()
    );
  }

  @Override
  public long approximateNumEntries() {
    return 0;
  }

  /**
   * Create a read-only key-value store for directly reading a single KStreams key/value
   * store instance. This fn will create a new store reader instance for the pss associated
   * with the kv store.
   * @param readerClient: the jni reader client to use for calls to rs3
   * @param checkpoint: the checkpoint to read from
   * @param partition: the partition of the kstreams store
   * @return a read-only key-value store
   */
  public static RS3ReadOnlyKeyValueBytesStore forCheckpoint(
      final JNIRs3ReaderClient readerClient,
      // todo: extend me to a list when we support alternative lss-pss mappings
      final PssCheckpoint checkpoint,
      final int partition
  ) {
    final UUID storeReaderId = UUID.randomUUID();
    readerClient.loadPssReaderFromCheckpoint(storeReaderId, checkpoint);
    return new RS3ReadOnlyKeyValueBytesStore(
        readerClient,
        storeReaderId,
        partition
    );
  }
}
