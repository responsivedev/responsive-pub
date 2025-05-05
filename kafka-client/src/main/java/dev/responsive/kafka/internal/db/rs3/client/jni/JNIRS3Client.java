package dev.responsive.kafka.internal.db.rs3.client.jni;

import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes;
import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.Delete;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import dev.responsive.kafka.internal.db.rs3.client.Put;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.StoreInfo;
import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.db.rs3.client.WindowedDelete;
import dev.responsive.kafka.internal.db.rs3.client.WindowedPut;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class JNIRS3Client implements RS3Client {
  private final dev.responsive.rs3.jni.RS3 rs3;

  private  JNIRS3Client(dev.responsive.rs3.jni.RS3 rs3) {
    this.rs3 = rs3;
  }

  @Override
  public CurrentOffsets getCurrentOffsets(UUID storeId, LssId lssId, int pssId) {
    final var offsets = rs3.getOffsets(storeId, lssId.id(), pssId);
    return new CurrentOffsets(
        offsets.written_offset(),
        offsets.flushed_offset()
    );
  }

  @Override
  public StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, long endOffset) {
    final var streamSender = new WalSegmentStreamSender(
        rs3,
        storeId,
        lssId.id(),
        pssId,
        endOffset,
        expectedWrittenOffset
    );
    return new StreamSenderMessageReceiver<>(
        streamSender,
        streamSender.future
    );
  }

  @Override
  public Optional<Long> writeWalSegment(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, long endOffset, List<WalEntry> entries) {
    final var streamSender = new WalSegmentStreamSender(
        rs3,
        storeId,
        lssId.id(),
        pssId,
        endOffset,
        expectedWrittenOffset
    );
    entries.forEach(streamSender::sendNext);
    streamSender.finish();
    try {
      return streamSender.future.get();
    } catch (final ExecutionException e) {
      throw e.getCause() instanceof RuntimeException ? (RuntimeException) e.getCause()
          : new RuntimeException(e);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<byte[]> get(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Bytes key) {
    final var row = rs3.kvGetValue(
        storeId,
        lssId.id(),
        pssId,
        key.get(),
        expectedWrittenOffset.orElse(null)
    );
    return Optional.ofNullable(row);
  }

  @Override
  public Optional<byte[]> windowedGet(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, WindowedKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Range<Bytes> range) {
    final var fromBound = range.start().map(new KVBoundMapper());
    final var toBound = range.end().map(new KVBoundMapper());
    final var iter = rs3.kvGetRange(
        storeId,
        lssId.id(),
        pssId,
        fromBound,
        toBound,
        expectedWrittenOffset.orElse(null)
    );
    return EmbeddedRs3Iterator.kvIterator(iter);
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> windowedRange(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, Range<WindowedKey> range) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<StoreInfo> listStores() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CreateStoreTypes.CreateStoreResult createStore(String storeId, CreateStoreTypes.CreateStoreOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PssCheckpoint createCheckpoint(UUID storeId, LssId lssId, int pssId, Optional<Long> expectedWrittenOffset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    rs3.close();
  }

  private static class WalSegmentStreamSender implements StreamSender<WalEntry> {
    private final dev.responsive.rs3.jni.RS3 rs3;
    private final UUID storeId;
    private final int lssId;
    private final int pssId;
    private final long endOffset;
    private final Optional<Long> expectedWrittenOffset;
    private CompletableFuture<Optional<Long>> future = new CompletableFuture<>();

    private dev.responsive.rs3.jni.RS3KVSegmentWriter kvSegmentWriter = null;

    public WalSegmentStreamSender(
        final dev.responsive.rs3.jni.RS3 rs3,
        final UUID storeId,
        final int lssId,
        final int pssId,
        final long endOffset,
        final Optional<Long> expectedWrittenOffset
    ) {
      this.rs3 = rs3;
      this.storeId = storeId;
      this.lssId = lssId;
      this.pssId = pssId;
      this.endOffset = endOffset;
      this.expectedWrittenOffset = expectedWrittenOffset;
    }

    @Override
    public void sendNext(final WalEntry msg) {
      handleFailure(() -> doSendNext(msg));
    }

    private void doSendNext(final WalEntry msg) {
      msg.visit(new WalEntry.Visitor() {
        @Override
        public void visit(final Put put) {
          maybeInitKvSegmentWriter();
          kvSegmentWriter.put(put.key(), put.value(), dev.responsive.rs3.jni.Ttl.defaultTtl());
        }

        @Override
        public void visit(final Delete delete) {
          maybeInitKvSegmentWriter();
          kvSegmentWriter.del(delete.key());
        }

        @Override
        public void visit(final WindowedDelete windowedDelete) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void visit(final WindowedPut windowedPut) {
          throw new UnsupportedOperationException();
        }
      });
    }

    private void maybeInitKvSegmentWriter() {
      if (kvSegmentWriter == null) {
        kvSegmentWriter = rs3.writeKVWalSegment(
            storeId,
            lssId,
            pssId,
            endOffset,
            expectedWrittenOffset.orElse(null)
        );
      }
    }

    @Override
    public void finish() {
      handleFailure(this::doFinish);
    }

    private void doFinish() {
      if (kvSegmentWriter != null) {
        kvSegmentWriter.close();
      }
      future.complete(rs3.getOffsets(storeId, lssId, pssId).flushed_offset());
    }

    @Override
    public void cancel() {
      handleFailure(this::doCancel);
    }

    private void doCancel() {
      if (kvSegmentWriter != null) {
        kvSegmentWriter.close();
      }
    }

    @Override
    public CompletionStage<Void> completion() {
      return future.thenAccept(v -> {});
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    private void handleFailure(final Runnable action) {
      try {
        action.run();
      } catch (final RuntimeException e) {
        future.completeExceptionally(e);
        throw e;
      }
    }
  }
}
