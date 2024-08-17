package dev.responsive.kafka.internal.db.pocket.client;

import java.util.List;
import java.util.Optional;

public interface PocketClient {
  CurrentOffsets getCurrentOffsets(LssId lssId, int pssId);

  StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync(
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      long endOffset
  );

  Optional<Long> writeWalSegment(
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      long endOffset,
      List<WalEntry> entries
  );

  Optional<byte[]> get(LssId lssId, int pssId, Optional<Long> expectedWrittenOffset, byte[] key);
}
