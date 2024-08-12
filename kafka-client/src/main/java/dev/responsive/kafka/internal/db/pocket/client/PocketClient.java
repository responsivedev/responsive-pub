package dev.responsive.kafka.internal.db.pocket.client;

import java.util.List;
import java.util.Optional;

public interface PocketClient {
  CurrentOffsets getCurrentOffsets(LssId lssId, int pssId);

  StreamSenderMessageReceiver<WalEntry, Long> writeWalSegmentAsync(
      LssId lssId,
      int pssId,
      long expectedWrittenOffset,
      long endOffset
  );

  long writeWalSegment(
      LssId lssId,
      int pssId,
      long expectedWrittenOffset,
      long endOffset,
      List<WalEntry> entries
  );

  Optional<byte[]> get(LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, byte[] key);
}
