package dev.responsive.kafka.internal.db.pocket.client;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PocketClient {
  CurrentOffsets getCurrentOffsets(UUID storeId, LssId lssId, int pssId);

  StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      long endOffset
  );

  Optional<Long> writeWalSegment(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      long endOffset,
      List<WalEntry> entries
  );

  Optional<byte[]> get(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      byte[] key
  );
}
