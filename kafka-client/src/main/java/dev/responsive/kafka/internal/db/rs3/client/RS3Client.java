/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client;

import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreResult;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface RS3Client {
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

  CreateStoreResult createStore(
      String storeId,
      CreateStoreOptions options
  );

  PssCheckpoint createCheckpoint(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset
  );

  void close();
}
