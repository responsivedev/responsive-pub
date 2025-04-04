package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.RS3TransientException;
import dev.responsive.kafka.internal.db.rs3.client.StreamSender;
import dev.responsive.kafka.internal.db.rs3.client.StreamSenderMessageReceiver;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class RS3Writer<K> implements RemoteWriter<K, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(RS3Writer.class);

  private final UUID storeId;
  private final RS3Client rs3Client;
  private final int pssId;
  private final LssId lssId;
  private final long endOffset;
  private final Optional<Long> expectedWrittenOffset;
  private final int kafkaPartition;
  private final List<WalEntry> retryBuffer = new ArrayList<>();
  private final StreamSenderMessageReceiver<WalEntry, Optional<Long>> sendRecv;

  protected RS3Writer(
      final UUID storeId,
      final RS3Client rs3Client,
      final int pssId,
      final LssId lssId,
      final long endOffset,
      final Optional<Long> expectedWrittenOffset,
      final int kafkaPartition
  ) {
    this.storeId = storeId;
    this.rs3Client = rs3Client;
    this.pssId = pssId;
    this.lssId = lssId;
    this.endOffset = endOffset;
    this.expectedWrittenOffset = expectedWrittenOffset;
    this.kafkaPartition = kafkaPartition;
    this.sendRecv = writeWalSegmentAsync();
  }

  protected abstract WalEntry createInsert(
      K key,
      byte[] value,
      long timestampMs
  );

  protected abstract WalEntry createDelete(
      K key
  );

  public long endOffset() {
    return endOffset;
  }

  public int kafkaPartition() {
    return kafkaPartition;
  }

  @Override
  public void insert(final K key, final byte[] value, final long timestampMs) {
    final var insertEntry = createInsert(key, value, timestampMs);
    maybeSendNext(insertEntry);
  }

  @Override
  public void delete(final K key) {
    final var deleteEntry = createDelete(key);
    maybeSendNext(deleteEntry);
  }

  private void maybeSendNext(WalEntry entry) {
    retryBuffer.add(entry);
    ifActiveStream(sender -> sender.sendNext(entry));
  }

  private void ifActiveStream(Consumer<StreamSender<WalEntry>> streamConsumer) {
    if (sendRecv.isActive()) {
      try {
        streamConsumer.accept(sendRecv.sender());
      } catch (final RS3TransientException e) {
        // Retry the stream in flush()
      }
    }
  }

  @Override
  public CompletionStage<RemoteWriteResult<Integer>> flush() {
    ifActiveStream(StreamSender::finish);

    return sendRecv.completion().handle((result, throwable) -> {
      Optional<Long> flushedOffset = result;

      var cause = throwable;
      if (throwable instanceof CompletionException) {
        cause = throwable.getCause();
      }

      if (cause instanceof RS3TransientException) {
        flushedOffset = writeWalSegmentSync(retryBuffer);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) throwable;
      } else if (cause != null) {
        throw new RuntimeException(throwable);
      }

      LOG.debug("last flushed offset for pss/lss {}/{} is {}", pssId, lssId, flushedOffset);
      return RemoteWriteResult.success(kafkaPartition);
    });
  }

  StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync() {
    return rs3Client.writeWalSegmentAsync(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        endOffset
    );
  }

  Optional<Long> writeWalSegmentSync(List<WalEntry> entries) {
    return rs3Client.writeWalSegment(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        endOffset,
        entries
    );
  }

}
