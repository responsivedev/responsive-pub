/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.db.WindowFlushManager;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.RS3Client;
import dev.responsive.kafka.internal.db.rs3.client.WalEntry;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

class RS3WindowFlushManager implements WindowFlushManager<Integer> {
  private final UUID storeId;
  private final RS3Client rs3Client;
  private final LssId lssId;
  private final RS3WindowTable table;
  private final int kafkaPartition;
  private final PssPartitioner pssPartitioner;
  private final RS3WindowedKeySerde keySerde;
  private final Map<Integer, Optional<Long>> writtenOffsets;

  private long streamTime;
  private final Map<Integer, Rs3WindowWriter> writers = new HashMap<>();

  public RS3WindowFlushManager(
      final UUID storeId,
      final RS3Client rs3Client,
      final LssId lssId,
      final RS3WindowTable table,
      final int kafkaPartition,
      final PssPartitioner pssPartitioner,
      final RS3WindowedKeySerde keySerde,
      final Map<Integer, Optional<Long>> writtenOffsets,
      final long initialStreamTime
  ) {
    this.storeId = storeId;
    this.rs3Client = rs3Client;
    this.lssId = lssId;
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.pssPartitioner = pssPartitioner;
    this.keySerde = keySerde;
    this.writtenOffsets = writtenOffsets;
    this.streamTime = initialStreamTime;
  }

  @Override
  public String tableName() {
    return table.name();
  }

  @Override
  public TablePartitioner<WindowedKey, Integer> partitioner() {
    return new PssTablePartitioner<>(pssPartitioner) {
      @Override
      public byte[] serialize(final WindowedKey key) {
        return keySerde.serialize(key);
      }
    };
  }

  @Override
  public RemoteWriter<WindowedKey, Integer> createWriter(
      final Integer pssId,
      final long consumedOffset
  ) {
    final var writer = new Rs3WindowWriter(
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
  public void writeAdded(final WindowedKey key) {
    streamTime = Long.max(streamTime, key.windowStartMs);
  }

  @Override
  public RemoteWriteResult<Integer> preFlush() {
    return RemoteWriteResult.success(null);
  }

  @Override
  public RemoteWriteResult<Integer> postFlush(final long consumedOffset) {
    for (final var entry : writers.entrySet()) {
      writtenOffsets.put(entry.getKey(), Optional.of(entry.getValue().endOffset()));
    }
    writers.clear();
    return RemoteWriteResult.success(null);
  }

  @Override
  public String failedFlushInfo(
      final long batchOffset,
      final Integer failedTablePartition
  ) {
    return String.format("");
  }

  @Override
  public String logPrefix() {
    return tableName() + ".rs3.flushmanager";
  }

  @Override
  public long streamTime() {
    return 0;
  }

  public Optional<Long> writtenOffset(final int pssId) {
    return writtenOffsets.get(pssId);
  }

  private static class Rs3WindowWriter extends RS3Writer<WindowedKey> {
    private final RS3WindowTable table;

    protected Rs3WindowWriter(
        final UUID storeId,
        final RS3Client rs3Client,
        final RS3WindowTable table,
        final int pssId,
        final LssId lssId,
        final long endOffset,
        final Optional<Long> expectedWrittenOffset,
        final int kafkaPartition
    ) {
      super(storeId, rs3Client, pssId, lssId, endOffset, expectedWrittenOffset, kafkaPartition);
      this.table = table;
    }

    @Override
    protected WalEntry createInsert(
        final WindowedKey key,
        final byte[] value,
        final long timestampMs
    ) {
      return table.insert(
          kafkaPartition(),
          key,
          value,
          timestampMs
      );
    }

    @Override
    protected WalEntry createDelete(final WindowedKey key) {
      return table.delete(
          kafkaPartition(),
          key
      );
    }
  }
}
