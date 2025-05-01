package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.rs3.client.CurrentOffsets;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.StoreInfo;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RS3ReaderClient {
  CurrentOffsets getCurrentOffsets(UUID storeId, LssId lssId, int pssId);

  Optional<byte[]> get(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      Bytes key
  );

  Optional<byte[]> windowedGet(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      WindowedKey key
  );

  KeyValueIterator<Bytes, byte[]> range(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      Range<Bytes> range
  );

  KeyValueIterator<WindowedKey, byte[]> windowedRange(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      Range<WindowedKey> range
  );

  List<StoreInfo> listStores();

  void close();
}
