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
      byte[] key
  );

  KeyValueIterator<Bytes, byte[]> range(
      UUID storeId,
      LssId lssId,
      int pssId,
      Optional<Long> expectedWrittenOffset,
      Range range
  );

  List<StoreInfo> listStores();

  CreateStoreResult createStore(
      String storeId,
      CreateStoreOptions options
  );

  void close();
}
