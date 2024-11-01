package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RS3KVFlushManager extends KVFlushManager {
  private static final Logger LOG = LoggerFactory.getLogger(RS3KVFlushManager.class);

  private final UUID storeId;
  private final RS3Client rs3Client;
  private final LssId lssId;
  private final RS3KVTable table;
  private final HashMap<Integer, Optional<Long>> writtenOffsets;
  private final int kafkaPartition;
  private final PssPartitioner pssPartitioner;
  private final HashMap<Integer, RS3KVWriter> writers = new HashMap<>();

  public RS3KVFlushManager(
      final UUID storeId,
      final RS3Client rs3Client,
      final LssId lssId,
      final RS3KVTable table,
      final HashMap<Integer, Optional<Long>> writtenOffsets,
      final int kafkaPartition,
      final PssPartitioner pssPartitioner
  ) {
    this.storeId = Objects.requireNonNull(storeId);
    this.rs3Client = Objects.requireNonNull(rs3Client);
    this.lssId = Objects.requireNonNull(lssId);
    this.table = Objects.requireNonNull(table);
    this.writtenOffsets = Objects.requireNonNull(writtenOffsets);
    this.kafkaPartition = kafkaPartition;
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public TablePartitioner<Bytes, Integer> partitioner() {
    return new PssTablePartitioner(pssPartitioner);
  }

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(
      final Integer pssId,
      final long consumedOffset
  ) {
    if (writers.containsKey(pssId)) {
      throw new IllegalStateException("already created writer for pss " + pssId);
    }
    final var maybeWrittenOffset = writtenOffsets.get(pssId);
    if (maybeWrittenOffset.isPresent()) {
      final var writtenOffset = maybeWrittenOffset.get();
      if (writtenOffset >= consumedOffset) {
        // This can happen after a restart, during restore because we restore from
        // the earliest written offset over all pss. For most pss we are restoring
        // from an offset earlier than the pss's last written offset. In that case,
        // we wait until we have crossed the written offset for the pss before sending
        // actual records.
        // TODO: we should be stricter when checking this case
        return new NoopWriter(kafkaPartition);
      }
    }
    final var writer = new RS3KVWriter(
        storeId,
        rs3Client,
        table,
        pssId,
        lssId,
        consumedOffset,
        writtenOffsets.get(pssId),
        kafkaPartition
    );
    writers.put(pssId, writer);
    return writer;
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
    for (final var entry : writers.entrySet()) {
      writtenOffsets.put(entry.getKey(), Optional.of(entry.getValue().endOffset()));
    }
    writers.clear();
    return super.postFlush(consumedOffset);
  }

  Optional<Long> writtenOffset(final int pssId) {
    return writtenOffsets.get(pssId);
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(final long consumedOffset) {
    // no op for rs3 store. offsets are updated by RS3Client.writeWalSegment
    return RemoteWriteResult.success(null);
  }

  @Override
  public String failedFlushInfo(final long batchOffset, final Integer failedTablePartition) {
    // TODO: fill me in with info about last written offsets
    return "";
  }

  @Override
  public String logPrefix() {
    return tableName() + ".rs3.flushmanager";
  }

  private static class NoopWriter implements RemoteWriter<Bytes, Integer> {
    private final int kafkaPartition;

    public NoopWriter(final int kafkaPartition) {
      this.kafkaPartition = kafkaPartition;
    }

    @Override
    public void insert(final Bytes key, final byte[] value, final long epochMillis) {
    }

    @Override
    public void delete(final Bytes key) {
    }

    @Override
    public CompletionStage<RemoteWriteResult<Integer>> flush() {
      return CompletableFuture.completedStage(RemoteWriteResult.success(kafkaPartition));
    }
  }

  private static class RS3KVWriter implements RemoteWriter<Bytes, Integer> {
    private final StreamSender<WalEntry> streamSender;
    private final CompletionStage<Optional<Long>> resultFuture;
    private RS3KVTable table;
    private final int pssId;
    private final LssId lssId;
    private final long endOffset;
    private final int kafkaPartition;

    private RS3KVWriter(
        final UUID storeId,
        final RS3Client rs3Client,
        final RS3KVTable table,
        final int pssId,
        final LssId lssId,
        final long endOffset,
        final Optional<Long> expectedWrittenOffset,
        final int kafkaPartition
    ) {
      this.table = Objects.requireNonNull(table);
      this.pssId = pssId;
      this.lssId = lssId;
      this.endOffset = endOffset;
      this.kafkaPartition = kafkaPartition;
      final var sendRecv = rs3Client.writeWalSegmentAsync(
          storeId,
          lssId,
          pssId,
          expectedWrittenOffset,
          endOffset
      );
      this.streamSender = sendRecv.sender();
      this.resultFuture = sendRecv.receiver();
    }

    long endOffset() {
      return endOffset;
    }

    @Override
    public void insert(final Bytes key, final byte[] value, final long epochMillis) {
      streamSender.sendNext(table.insert(kafkaPartition, key, value, epochMillis));
    }

    @Override
    public void delete(final Bytes key) {
      streamSender.sendNext(table.delete(kafkaPartition, key));
    }

    @Override
    public CompletionStage<RemoteWriteResult<Integer>> flush() {
      streamSender.finish();
      return resultFuture.thenApply(
          flushedOffset -> {
            LOG.debug("last flushed offset for pss/lss {}/{} is {}", pssId, lssId, flushedOffset);
            return RemoteWriteResult.success(kafkaPartition);
          }
      );
    }
  }
}
