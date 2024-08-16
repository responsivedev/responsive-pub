package dev.responsive.kafka.internal.db.pocket;

import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.pocket.client.LssId;
import dev.responsive.kafka.internal.db.pocket.client.PocketClient;
import dev.responsive.kafka.internal.db.pocket.client.StreamSender;
import dev.responsive.kafka.internal.db.pocket.client.WalEntry;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PocketKVFlushManager extends KVFlushManager {
  private static final Logger LOG = LoggerFactory.getLogger(PocketKVFlushManager.class);

  private final PocketClient pocketClient;
  private final LssId lssId;
  private final PocketKVTable table;
  private final HashMap<Integer, Long> writtenOffsets;
  private final int kafkaPartition;
  private final PssPartitioner pssPartitioner;
  private final HashMap<Integer, PocketKVWriter> writers = new HashMap<>();

  public PocketKVFlushManager(
      final PocketClient pocketClient,
      final LssId lssId,
      final PocketKVTable table,
      final HashMap<Integer, Long> writtenOffsets,
      final int kafkaPartition,
      final PssPartitioner pssPartitioner
  ) {
    this.pocketClient = Objects.requireNonNull(pocketClient);
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
    return new TablePartitioner<Bytes, Integer>() {
      @Override
      public Integer tablePartition(int kafkaPartition, Bytes key) {
        return pssPartitioner.pss(key.get(), kafkaPartition);
      }

      @Override
      public Integer metadataTablePartition(int kafkaPartition) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public RemoteWriter<Bytes, Integer> createWriter(
      final Integer pssId,
      final long consumedOffset
  ) {
    if (writers.containsKey(pssId)) {
      throw new IllegalStateException("already created writer for pss " + pssId);
    }
    final var writer = new PocketKVWriter(
        pocketClient,
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
  public RemoteWriteResult<Integer> postFlush(long consumedOffset) {
    for (final var entry : writers.entrySet()) {
      writtenOffsets.put(entry.getKey(), entry.getValue().endOffset());
    }
    writers.clear();
    return super.postFlush(consumedOffset);
  }

  Optional<Long> writtenOffset(final int pssId) {
    return Optional.ofNullable(writtenOffsets.get(pssId));
  }

  @Override
  public RemoteWriteResult<Integer> updateOffset(long consumedOffset) {
    // no op for pocket store. offsets are updated by PocketClient.writeWalSegment
    return RemoteWriteResult.success(null);
  }

  @Override
  public String failedFlushInfo(long batchOffset, Integer failedTablePartition) {
    // TODO: fill me in with info about last written offsets
    return "";
  }

  @Override
  public String logPrefix() {
    return tableName() + ".pocket.flushmanager";
  }

  private static class PocketKVWriter implements RemoteWriter<Bytes, Integer> {
    private final StreamSender<WalEntry> streamSender;
    private final CompletionStage<Long> resultFuture;
    private PocketKVTable table;
    private final int pssId;
    private final LssId lssId;
    private final long endOffset;
    private final int kafkaPartition;

    private PocketKVWriter(
        final PocketClient pocketClient,
        final PocketKVTable table,
        final int pssId,
        final LssId lssId,
        final long endOffset,
        final Long expectedWrittenOffset,
        final int kafkaPartition
    ) {
      this.table = Objects.requireNonNull(table);
      this.pssId = pssId;
      this.lssId = lssId;
      this.endOffset = endOffset;
      this.kafkaPartition = kafkaPartition;
      final var sendRecv = pocketClient.writeWalSegmentAsync(
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
    public void insert(Bytes key, byte[] value, long epochMillis) {
      streamSender.sendNext(table.insert(kafkaPartition, key, value, epochMillis));
    }

    @Override
    public void delete(Bytes key) {
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
